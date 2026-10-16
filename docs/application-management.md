# Application management

Applications are managed through an authenticated HTTP API (`celld
application-api`) and a thin CLI (`celld application`). Host setup, images,
packaging and the service bindings a handler can use are in
[application-setup.md](application-setup.md).

The API manages one independently versioned metadata record per application
in the fleet bucket. An application ID is the authorization boundary; customer
accounts, organizations and billing belong to the integrating platform. Nodes
started with `--application-vms` discover current application records and run
executable handlers in Firecracker VMs. Nodes with `--application-domain DOMAIN`
serve application assets without VM configuration, runtime images or KVM.

## Start the API

Build on Linux (see the setup guide), then:

```sh
celld application-api --auth-file .celld/application-auth.json \
  --platform-domain apps.example.com \
  --bucket s3://YOUR_BUCKET/YOUR_FLEET --listen 127.0.0.1:8090
```

The API uses the original S3/GCS/Azure adapter, credential chain, fleet prefix
and conditional-write semantics. `--endpoint` and `--region` are the existing
fleet flags. It is a separate listener from the node's peer/operator listener.
Terminate TLS at a reverse proxy for remote access. The CLI requires HTTPS
except for loopback HTTP and refuses redirects.

Authorization is a host-local JSON file loaded at startup. Restart the API to
change grants or revoke credentials. Generate random tokens with at least 32
bytes of entropy; the file stores their lowercase SHA-256 digests:

```json
{
  "credentials": [
    {
      "token_sha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
      "permission": {"scope": "operator"}
    },
    {
      "token_sha256": "REPLACE_WITH_ANOTHER_TOKEN_HASH",
      "permission": {"scope": "application", "application": {"app": "shop"}, "access": "manage"}
    },
    {
      "token_sha256": "REPLACE_WITH_A_THIRD_TOKEN_HASH",
      "permission": {
        "scope": "application",
        "application": {"app": "shop"},
        "access": "read"
      }
    }
  ]
}
```

Operator credentials manage all applications. Application credentials manage
or read only one application; their listing reads just that application. Unknown fields/scopes and duplicate token
hashes reject startup. Authentication and path authorization precede body
extraction, so an unauthorized upload is rejected without receiving its body.
Never place raw tokens or provider credentials in manifests or artifacts.

## CLI

Set `CELLD_APPLICATION_API` to the API origin and `CELLD_APPLICATION_TOKEN` to
the raw token (32 to 4096 printable bytes). Tokens are not command arguments.
Every successful CLI response is JSON on stdout; logs and errors use stderr.

```sh
celld application list
celld application create --app shop
celld application get --app shop
celld application resource --app shop --file database.json
celld application artifact --app shop --file handler.img
celld application assets --app shop --directory ./public [--file project.json]
celld application stage --app shop --file deployment.json
celld application manifest --app shop --deployment v1
celld application activate --app shop --deployment v1
celld application deactivate --app shop
celld application secrets --app shop
celld application secret-put --app shop --secret api-token --file token.txt
celld application secret-delete --app shop --secret api-token
celld application limits --app shop --file limits.json
celld application limits-reset --app shop
```

`--api URL` overrides the environment. Identifiers (app, deployment,
secret, resource, handler, trigger) are 1-128 ASCII letters, digits, dashes or
underscores. Edits read the application's own revision once and send it back; a conflict
stops the command without retry. Create sends an absent revision without a
fleet listing. `list --limit 1 --cursor TOKEN` returns one page; pass its
`next_cursor` unchanged with the same limit to resume.

## Lifecycle

1. **create** the application (its metadata record; no resources yet).
2. **resource** registers one application-owned resource per call. The file is
   `{"resource":{"kind":"d1","id":"database"},"configuration":{}}`. Kinds:
   `durable_object`, `d1`, `kv`, `queue`, `cron`, `workflow`, `r2`,
   `container`, `worker_loader`, `static_assets`. Registering reserves the
   identity and its configuration; it does not provision anything. Resources are
   independent of handler versions and binding aliases, and re-registering the
   same identity with a different configuration is refused. `configuration` is
   `{}` for every kind except `container` (see the setup guide). A queue
   resource's id is the queue name, a cron resource's id is its trigger id, a
   workflow resource's id is the workflow name and an R2 resource's id is the
   actual bucket ID. R2 objects use that bucket with Celld's existing key escaping, so applications
   binding the same ID share objects. The fleet bucket ID is forbidden. The
   integrating platform controls which IDs an application may register.
3. **artifact** uploads the handler image (a read-only EROFS image of its files, see the setup guide)
   and prints its SHA-256 as `artifact_sha256`. **assets** builds and uploads a
   static-asset directory with the original deploy asset pipeline and prints
   `assets` reference to paste into a deployment's top-level `assets` field for
   a static route, or into a handler's `worker_config.assets` for mixed routing.
4. **stage** stores an immutable deployment manifest (`deployment.json`, the
   `celld-applications::Deployment` contract; see below). Staging validates the
   contract only; it does not activate anything.
5. **activate** publishes a staged deployment through the application's own CAS. It requires
   the immutable manifest, every handler artifact present with a matching
   digest and every referenced resource and secret registered. The first
   activation of a handler also assigns it a public route id (see below). A refusal does not replace the active deployment. Prepared index candidates
   can remain after an interrupted or refused update; they admit no execution
   without matching committed metadata. Activating
   an older staged deployment is the rollback. To remove a handler, stage and
   activate a deployment that lacks it. To disable a trigger, activate a
   deployment without it (there is no `enabled` field).
6. **deactivate** withdraws the active deployment. Resources, secrets, limits and
   staged deployments are kept; persistent data is never deleted by code changes
   or deactivation.

`deployment.json`:

```json
{
  "manifest_version": 1,
  "application": {"app": "shop"},
  "id": "v1",
  "handlers": [{
    "id": "web",
    "language": "javascript",
    "entrypoint": "main.mjs",
    "artifact_sha256": "<from the artifact command>",
    "runtime_image_sha256": "<approved runtime image digest>",
    "http": true,
    "bindings": {"DB": {"kind": "resource", "resource": {"kind": "d1", "id": "database"}}},
    "environment": {"MODE": "prod"},
    "secrets": {"PRIVATE_TOKEN": "api-token"},
    "worker_config": {}
  }],
  "triggers": [
    {"kind": "cron", "id": "tick", "expression": "* * * * *", "handler": "web"},
    {"kind": "queue", "queue": "jobs", "handler": "web"}
  ]
}
```

The only handler language is `javascript` (compile TypeScript first). Each
handler pins an entrypoint (artifact-relative path), the artifact digest and a
host-approved runtime image digest. `bindings` map an alias either to a
registered resource (`{"kind":"resource","resource":{...}}`) or to another
handler of the same deployment (`{"kind":"handler","handler":"id"}`, a service
binding). Binding, environment and secret aliases may not collide. Cron and
queue triggers need a registered `cron` (id = trigger id) or `queue` resource.
`worker_config` carries the original Celld
worker metadata; its per-service shapes are in the setup guide.

## Handler URLs

A handler with `"http": true` is served from public ingress while its
deployment is active, at `<route id>.<platform domain>`. The route id is
assigned by the host, never chosen by application code: 128 random bits as 32 lowercase
hex digits, generated the first time a handler id is activated in an
application and stored in the application record (`route_ids`). It stays the same across
replacement, rollback and deactivation, is never reused, and a manifest cannot
carry one (an unknown field is refused). The platform domain is node
configuration (`platform_domain` in the node VM configuration; pass the same
value to `celld application-api --platform-domain` so `get` can print the
hostnames). Every request to the handler's hostname reaches that handler: the
handler sees the whole path, and there are no application-selected path prefixes. A
host under the platform domain with an unknown id, an inactive application or a
handler that is not `http` answers 404, never the legacy Worker. `get` prints
`hostnames`, a map from each served handler of the active deployment to its
hostname. Old manifests with `http_routes` are refused ("http_routes is no
longer supported"). Custom domains and per-deployment preview URLs are not
implemented.

## Static-only applications

Build the `celld` binary on Linux, start the management API as above, and start
the node against the same fleet bucket and platform domain:

```sh
celld --bucket s3://YOUR_BUCKET/YOUR_FLEET --application-domain apps.example.com
celld application create --app site
celld application assets --app site --directory ./public
```

Copy the printed `assets` object into a deployment manifest. No handler image,
entrypoint, runtime image, resource binding or executable handler is needed:

```json
{
  "manifest_version": 1,
  "application": {"app": "site"},
  "id": "v1",
  "assets": {
    "index": "assets.json",
    "sha256": "<sha256 from the assets command>",
    "file_count": 1,
    "total_bytes": 42
  }
}
```

Use the actual `file_count` and `total_bytes` from that command, then publish:

```sh
celld application stage --app site --file deployment.json
celld application activate --app site --deployment v1
celld application get --app site
```

`get` prints `asset_hostname`. This separate host-assigned route is retained in
`application.asset_route_id`; replacement and rollback keep the URL, and
deactivation withdraws it without deleting uploads. The node can start before
any legacy `CURRENT` deployment exists. It uses the original asset resolver,
application-scoped immutable artifacts and local cache. GET/HEAD, content
types, conditional/range responses, `_headers`, `_redirects`, HTML handling and
configured SPA/404 behavior are unchanged. Missing paths return a terminal
404; other methods return 405 with `Allow: GET, HEAD` before reading the body.

Activation verifies the index and every body's checksum before publishing.
Missing, corrupt or foreign assets leave the active deployment unchanged.
Static routes refuse `run_worker_first`; deployments without handlers also
refuse cron/queue triggers. A mixed deployment can carry top-level `assets`
and executable handlers: its static URL always resolves on the host, while
each handler URL retains assets-first and explicit worker-first dispatch into
Firecracker. Dynamic execution requires `--application-vms`; those nodes use
the `platform_domain` in their VM configuration and also serve static routes.
Use the same domain for the API. `--application-domain` and
`--application-vms` cannot be combined on one node.

## HTTP API

All requests require one `Authorization: Bearer TOKEN` header. `BASE` is
`/v1/applications/APP`.

| Method/path | Body/result |
|---|---|
| `GET /v1/applications?limit=100&cursor=TOKEN` | One page: `applications:[{"application":{...},"revision":{"token":"..."}}]`, `next_cursor` (null at the end); no fleet revision |
| `GET BASE` | `application` (metadata, including `route_ids` and `asset_route_id`), `hostnames` (served handlers), `asset_hostname` (active static route, otherwise null), `runtime` (node-observed state), `revision` |
| `POST BASE` | `{"revision":{"token":null}}` to create an absent application |
| `PUT BASE/resources` | `{"revision":{"token":"..."},"resource":{...}}` |
| `PUT BASE/deployments/VERSION` | Full immutable deployment manifest; path and body identities must agree |
| `GET BASE/deployments/VERSION` | Stored manifest |
| `PUT BASE/artifacts/SHA256` | Raw bytes, checked against the path digest, maximum 64 MiB |
| `PUT BASE/active` | `{"revision":{"token":"..."},"deployment":"VERSION"}`; activate or roll back |
| `DELETE BASE/active` | `{"revision":{"token":"..."}}`; preserves resources and staged deployments |
| `GET BASE/secrets` | Application revision and secret names with opaque versions; no values |
| `PUT BASE/secrets/NAME` | `{"revision":{"token":"..."},"value":"..."}`; create or rotate |
| `DELETE BASE/secrets/NAME` | `{"revision":{"token":"..."}}`; remove the current reference |
| `PUT BASE/limits` | `{"revision":{"token":"..."},"limits":{"size_class":"basic","invocation_timeout_ms":3000}}` |
| `DELETE BASE/limits` | `{"revision":{"token":"..."}}`; reset application limits |

Ordinary JSON bodies are capped at 1 MiB; at most eight management requests run
at once, each bounded by a request timeout (300 seconds), so a stalled client
cannot hold a slot indefinitely. Application writes require their own exact observed revision and
use provider CAS. `409` means re-read and review the intended edit; the CLI never
retries blindly. An uncertain provider result is reconciled only by exact-content
readback. Manifests are immutable by application/deployment id and artifacts by
application/digest. An application metadata record is limited to 8 MiB;
combined fleet metadata has no 8 MiB ceiling.
Unrelated applications update independently. Operator listings default to 100
records, accept 1-256, and cap a page at 16 MiB. An oversized page returns 400;
restart the listing with a smaller limit. Cursors bind the listing and limit.
Application credentials return zero or one entry and reject cursors without
listing the fleet. Invalid cursors or unknown query fields return 400.

Cell data, leases and replication retain the original Celld mechanisms. Old
application IDs containing `tenant`, tenant grants, tenant CLI flags and the
legacy fleet catalog are unsupported; there is no migration reader.

## Status

`get` returns two separate things. `application` is what is meant to run
(resources are registered, not provisioned). `runtime` is what nodes reported:
one record per node that has observed anything, with `node`,
`observed_at_unix_secs` and, per handler, `deployment`, `handler` and a state of
`launching`, `serving`, `draining` or `failed` with a fixed host-chosen reason
(unapproved image, launch failed, unproved reap; raw errors stay in host logs).
Each node overwrites its own record, and deletes it when it has nothing to
report and at drain. An empty list means no node has reported; an old timestamp
means a crashed node may have left a stale record (at most 64 records are
returned). Handlers active in application metadata but not yet launched have no entry.

## Secrets and limits

`secret-put` reads the exact UTF-8 file contents, including trailing newlines.
Values are at most 16 KiB with no NUL; an application registers up to 256 names.
There is no secret-value read API: inspection and listing show names and random
opaque versions only. Read grants may inspect that metadata; changes need manage
permission.

A handler maps aliases to secret names (`"secrets":{"PRIVATE_TOKEN":"api-token"}`).
At cold launch the trusted host resolves those names from the application's own
metadata snapshot and merges the values into the guest's private bootstrap
(`env.PRIVATE_TOKEN`), without changing the stored manifest or its digest.
Rotation affects launches that read the new snapshot; running VMs keep their
pinned values. Deletion returns `409` while an active deployment references the
name. Staged deployments may name a deleted secret, but a launch then refuses.
Values are stored as plaintext JSON in the private fleet bucket; there is no
KMS layer, and old versions, losing-writer blobs, garbage collection and
fleet-wide revocation are not implemented. An application can read and disclose
its own secrets. The private bootstrap is capped at 64 KiB, so many large
secrets make a launch refuse before any VMM starts.

`limits.json` is the policy object itself (no revision wrapper):

```json
{"size_class":"basic","invocation_timeout_ms":3000}
```

`size_class` selects one of six fixed classes (`basic` by default):

| Class | CPU allocation | Memory |
|---|---|---|
| `lite` | 1/16 vCPU | 256 MiB |
| `basic` | 1/4 vCPU | 1 GiB |
| `standard-1` | 1/2 vCPU | 4 GiB |
| `standard-2` | 1 vCPU | 6 GiB |
| `standard-3` | 2 vCPUs | 8 GiB |
| `standard-4` | 4 vCPUs | 12 GiB |

Unknown class names, container aliases (`dev`, `standard`) and raw `vcpus` or
`memory_mib` fields are refused. Fractional classes have one Firecracker vCPU
with their CPU allocation enforced by the VM's own cgroup v2 `cpu.max` quota.
CPU quotas are maxima. Admission does not reserve host CPU cores, and contention
can reduce service below the class ceiling; quotas promise no minimum share.
Startup is the exception. Until its Ready frame, a fractional-class VM runs at its
whole vCPU while restoring, prewarming and loading its application, if the node
has boost slots free (half its CPUs, counted in vCPUs). A waiting pool VM and a
ready VM are held to the class quota.
Classes have no per-class disk allocation; handler and runtime drives remain
read-only. The node admits the selected class only if its capacity permits it.

`invocation_timeout_ms` is 1-300000 and can only shorten the caller's deadline. The
deadline starts at invocation admission after boot; startup, cleanup and
physical reaping are separate and can outlast it. Limits are pinned at launch,
so updates and `limits-reset` apply to newly launched VMs. Scratch space,
concurrency, trigger quotas and live reconciliation of running VMs are not
implemented.

## Handler VM inactivity policy

The selected lifecycle policy retires an assigned handler VM after 30 seconds of
inactivity, measured after all admitted work drains. Eligible VMs freeze
during that window, resume for a new event before expiry, and can be evicted
earlier under pressure. A new event starts another idle interval only after its
work drains. Running invocations, streams and `waitUntil` work are protected.

Once reaped, the next invocation uses a fresh VM; volatile globals and in-memory
caches are lost. Published deployments and durable service data remain. The node
setup example sets `idle_ttl_secs` to 30. An unused lease or retained client does
not extend the idle deadline, and an expired client cannot revive its old VM.
Pause/resume keeps the full memory reservation by default. Optional
`memory_guard.idle_cache` allows a confirmed paused VM to use an enforced retained
envelope; resume first requires full reservation and the full leaf ceiling.
Cache byte limits and safety margins require host/workload calibration; see
[setup](application-setup.md). Status separates `fullExecutionMib`,
`retainedLimitBytes`, `retainedVms`, `cachedNominalMib`, `retainedOverheadBytes`
and guarded `physicalBytes` under `memoryReservations`. These counters describe
node capacity. Startup-pool VMs remain running. This timeout is a node policy,
not a tenant billing setting.

Executing owners require their full ceilings; warm-cache occupancy does not
promise simultaneous execution capacity. Production overhead, cache margins and
capacity require measurements on your provisioned host.

Outstanding timers and unconsumed response bodies remain live work. Drain or
cancel response bodies before expecting a VM to pause. Host-owned alarms and
persisted workflow wakes use readmission; guest pause does not provide durable
scheduling for arbitrary guest timers.
