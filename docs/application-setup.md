# Application platform setup

How to build, approve and run JavaScript applications in Firecracker VMs on a
Celld node, and which services they can bind. Day-to-day management (API, CLI,
secrets, limits, status) is in [application-management.md](application-management.md).

Static-only applications need the Linux `celld` binary and a fleet bucket;
start their node with `--application-domain DOMAIN`. They need no KVM,
Firecracker, jailer, guest kernel or runtime image. The VM prerequisites below
apply to executable handlers. See the management guide's static-only walkthrough.

Handlers run original Celld/V8 inside a credential-free microVM. The host keeps
authorization, ownership, SQLite, replication, proof and output publication.

## Host requirements

- Linux x86_64 with `/dev/kvm`. Application VMs are Linux-only
  (`--application-vms` refuses elsewhere). The celld node runs as root, because
  the jailer chroots, drops to `uid`/`gid` and sets up the VM sandbox.
- Firecracker and jailer at absolute paths (v1.17.0 is supported).
- An uncompressed `vmlinux` guest kernel (at most 128 MiB) with virtio block,
  vsock, EROFS, entropy, devtmpfs and ACPI/VMGenID support, booted with
  `pci=off`. PCI core stays enabled because ACPI depends on it; the guest has
  no PCI devices. The kernel is operator-supplied and approved by SHA-256.
  Build one with `scripts/application/build-guest-kernel.sh` and
  `guest-kernel.config` (see [Kernel build](#kernel-build)). A kernel without
  EROFS cannot boot the runtime images.
- Build host tools for the image builders: Python 3.11+ (3.12 qualified),
  `mkfs.erofs` and `fsck.erofs` (erofs-utils 1.7.1, at `/usr/bin`),
  `strip` and `readelf` (binutils). `ldd` is needed only when packaging a dynamic
  ELF; the static release path needs no guest libraries. Python is a build-host
  utility only; no Python executable is packaged into a guest.
- A fleet bucket (S3/GCS/Azure, or the local development bucket) as for any
  Celld fleet. Nodes and the management API share its per-application metadata records.
- The guest has no network device. All egress is brokered by the host and
  passes the original output gate.
- Containers only: a Linux Docker (or Podman) daemon the node can reach through
  `DOCKER_HOST=unix:///path/to/sock`, with the `celld-fence` image available
  (see Containers below). Docker Desktop's VM cannot be used: container bridge
  IPs must be reachable from the node. On WSL, run a separate native daemon on
  `/run/celld-docker.sock`; `scripts/application/setup-celld-docker.sh`
  installs one without touching Docker Desktop.

## Build

```sh
cargo build --locked --bin celld
cargo rustc --release --locked -p celld --bin celld-guest -- -C target-feature=+crt-static
python3 -B crates/applications/guest/build-application-image.py --check
```

`celld` is the node, API and CLI. `celld-guest` is the native V8 guest that
runs inside the VM. Build the Cargo target and runtime on an ext4 filesystem;
a Windows checkout is source input only.

## Runtime image and kernel approval

The runtime image is an uncompressed, read-only EROFS holding the fully stripped
static release `celld-guest`, hard-linked as `/sbin/init`. The guest itself runs
as PID 1; there is no shell init or loader library. It mounts proc/sys and 16 MiB
of tmpfs each at `/run` and `/tmp`, and refuses a writable root. It has no Python
executable, adapter or SDK:

```sh
python3 -B crates/applications/guest/build-check-image.py \
  /var/lib/celld/runtime.img target/release/celld-guest
```

The builder prints the image SHA-256 and writes it to a sibling
`runtime.sha256`. It refuses to overwrite an existing output. Builds are
byte-reproducible for the same guest binary and erofs-utils (fixed UUID,
timestamps, block size, ownership and permissions; no compression or xattrs),
so the digest is the approval handle. Rebuilding celld
changes the guest binary and therefore the digest: approve the new digest on
nodes and use it in new manifests; running deployments keep their pinned image.

Application handlers are not packaged here; they arrive as artifacts.

Approval is trusted node configuration, never application data. A handler
selects a runtime only by naming an approved digest in `runtime_image_sha256`;
an unapproved, mismatched or symlinked image refuses launch before any VMM
starts. During template preparation the host copies and hashes the actual
kernel (at most 128 MiB) and runtime (at most 1 GiB) into private files. Claims
hard-link the already approved read-only runtime into each jail. Template keys
include runtime and kernel digests and Firecracker version; changed inputs
require fresh templates and all handler/shared drives must use EROFS.

## Kernel build

```sh
# A fresh directory is required; the script never replaces an installed kernel.
bash scripts/application/build-guest-kernel.sh /abs/new-kernel-build
```

The script downloads official Linux 6.1.186 from
[cdn.kernel.org](https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.1.186.tar.xz),
checks the pinned source SHA-256 and its entry in the published checksum list,
applies the shipped config and asserts the required devices and excluded
subsystems. This verifies SHA-256 over HTTPS; it does not claim an independently
verified GPG signature. Build tools are GCC, make, binutils, flex, bison, bc,
Python, curl, tar, libelf-dev and libssl-dev. Fixed build timestamp/user/host/
version and an empty build salt remove host metadata. The output retains
`artifacts/vmlinux`, the resolved `kernel.config` and a `provenance.txt`.
Approve that exact kernel hash with its matching EROFS runtime; do not infer
approval from application data.

The config keeps virtio MMIO/blk/vsock, serial, ACPI/PCI core, VMGenID, KVM clock
and entropy, devtmpfs/proc/sysfs/tmpfs, ELF and thread APIs. EROFS is the only
disk filesystem; ext4, squashfs, journals, network devices and unrelated drivers
are disabled. Upstream Kconfig selects PNP, PERF_EVENTS and HOTPLUG_CPU cores.
SERIAL_8250_PNP discovers the serial console through ACPI so it uses the correct
IRQ; disabling it makes userspace stderr fail with EIO. Built-in virtio
ballooning and page reporting are enabled, but device reporting does not
authorize smaller execution reservations.

## Node VM configuration

A node runs application VMs only when started with `--application-vms FILE`;
without it the node does not host application deployments and behaves as before.

```json
{
  "jailer": "/usr/local/bin/jailer",
  "firecracker": "/usr/local/bin/firecracker",
  "kernel": {"path": "/abs/approved-vmlinux-erofs", "sha256": "APPROVED_KERNEL_DIGEST"},
  "runtimes": [{"path": "/var/lib/celld/runtime.img", "sha256": "RUNTIME_IMAGE_DIGEST"}],
  "uid": 1000, "gid": 1000,
  "max_vms": 8, "max_memory_mib": 12288, "idle_ttl_secs": 30,
  "startup_pool": 0,
  "metadata_cache_entries": 256, "metadata_cache_bytes": 16777216,
  "platform_domain": "apps.example.com",
  "container_fence_image": "celld-image:<64 hex>"
}
```

Metadata caches default to 256 entries and 16 MiB of charged metadata per node.
`metadata_cache_entries` accepts 1-4096; `metadata_cache_bytes` accepts 1-67108864.
Cached entries expire after one second; absent indexes are not cached because
a read can race their first publication. Resident cells and admitted events
retain metadata pins under the existing admission limits; evicting a discovery
cache entry does not interrupt accepted work. Fresh VM admission checks the
application's current active deployment.

All paths are absolute. `runtimes` digests must be distinct. `uid`/`gid` must be
nonzero (the jailed process identity). Applications select one of the six named
classes in the management guide; raw per-VM `vcpus` and `memory_mib` settings are
refused. Each VM has its own cgroup v2 CPU quota, so the jailer needs write access
to a delegated cgroup v2 hierarchy. `max_vms` (1-256), `max_memory_mib` (at least
12288, to build the largest class serially) and `idle_ttl_secs` (1-3600) bound the
node: capacity is reserved before launch, released only after the jailer/VMM is
reaped, and an unproved reap stays charged (quarantined). Idle VMs are evicted
LRU at capacity and by TTL otherwise. Active events remain protected; unused
leases do not extend the assigned VM's verified idle deadline. `platform_domain` (required, a lowercase DNS name with at least two
labels) is the domain handlers are served under: a handler with `"http": true`
answers at `<route id>.<platform_domain>`, where the host assigns the id at the
first activation (management guide, "Handler URLs"). Point wildcard DNS and TLS
for `*.<platform_domain>` at the nodes' public listener; hosts under it never
fall through to the Worker. `container_fence_image` is optional, node-only, and required only when
applications use Containers.

The example sets the existing idle timeout to 30 seconds. An idle handler VM is
retired and reaped after that interval; later work launches a fresh VM from a
startup snapshot or claims an unassigned pool VM. The idle timeout does not kill
an active invocation, response stream or pending `waitUntil` work.

The lifecycle freezes eligible handler VMs during this idle window and
thaws them if work arrives before expiry; pressure may evict them sooner. Host
freeze requires a host writer fence and a correlated guest idle proof. New work,
including retained clients, waits for the owning supervisor to resume the VM;
that wait consumes the invocation's original deadline. Full memory reservations
remain charged while paused unless enforced retained caching is configured below.
An expired VM becomes ineligible before physical
reaping finishes. Startup-pool VMs stay running. The 30-second assigned-handler timeout is
separate from startup-pool refill and artifact-cache retention.

`memory_guard` is optional and keeps existing nominal `max_memory_mib` accounting.
Its explicit byte values are `max_bytes` (aggregate physical limit), `high_bytes`
(pressure threshold), `overhead_bytes` (per-VM host allowance), and
`shared_cache_bytes` (reserved shared-cache allowance). Choose them from measurements
on the provisioned host; no production defaults are supplied. The high threshold
must be below max and above half of max, and the largest class plus allowances
must fit. Guarded VM leaves have a hard ceiling, no swap, and per-VM OOM grouping;
the host control plane remains outside their stable parent. Pressure blocks new
starts/refills, reaps startup guests first, then eligible assigned idle LRU, and
requires a stable low-pressure interval before admission recovers. Proven leaf
OOM is reported as `memory limit exhausted`; unproved reap remains quarantined.
Pool status includes guard counters, the pressure latch and lifecycle counts.

Optional `memory_guard.idle_cache` takes explicit `max_bytes` and
`retained_margin_bytes`. After a confirmed pause, the supervisor may lower the
owned leaf's hard ceiling to measured current memory plus that margin, verify
the installed limit, and charge it against the cache budget. All VM overhead
and shared-cache allowances remain charged; nominal `max_memory_mib` stays unchanged.
Before resume, the supervisor reserves the full execution budget and restores the
full leaf ceiling. A blocked wake waits within its original deadline and idle
expiry; eligible idle VMs may be evicted to make room. Capacity is released only
after physical reap. Missing or uncertain enforcement retains full charging and
retires the VM. Choose cache limits and margins from host and workload
measurements; no production defaults are supplied. Warm-cache occupancy does
not promise simultaneous execution capacity, and production overhead, cache
margins and capacity require measurements on your provisioned host.

At node startup, the node sequentially boots and captures an unassigned guest
(`celld-guest --template`) for every approved runtime image and all six classes.
It also builds the shared Pyodide drive and base snapshot for every runtime,
including when no Python handler is active yet. These caches are built on the
node, with each builder charged against its capacity until physical reap; none
is embedded in the release. Both HTTP surfaces return 503 during preparation,
and the node takes no ownership or application requests until the caches exist.
`application_caches_ready` records the elapsed startup time. Application launches
then reuse their prepared caches and restore a snapshot or claim a pool VM.
Templates live root-owned and read-only in a private directory under the node's
temporary directory, are keyed by runtime digest, VM size, kernel digest,
Firecracker version and memory-device profile, and are discarded on restart and pool shutdown. A failed
startup stops the node from becoming ready. A missing cache or failed restore
refuses the request; production requests do not build templates or fall back to
cold boot. `vm_startup` records phase timings and whether a launch restored or
claimed a pool VM. Pool directories that a killed node left in the temporary
directory are removed when a later node starts (a running node holds a lock on
its own, and directories under a minute old are spared).

`guest_application_initialization` separates artifact loading, engine setup,
configuration and worker loading. `guest_worker_initialization` separates
isolate, operations, prelude, harness, globals, module evaluation, bindings and
heap settlement. Their totals are nested.

`startup_pool` (optional, default 0) keeps that many restored VMs per template
ready for a claim. With 0 each launch restores on demand. With `N > 0` the node
refills each template it has needed, in the background, up to `N` waiting VMs
with the guest agent connected and no application; a cold launch claims one
(`path` = `pool`) instead of restoring. A waiting VM holds no application data,
and a claimed VM serves only its handler and is destroyed at retirement; it never
returns to the pool. Waiting, restoring, reaping and quarantined pool VMs count
at their actual class memory against `max_vms` and `max_memory_mib`; refill leaves
room for one more default-class VM. A launch that
finds the node full evicts a waiting VM first (before any idle handler VM and
before refusing with capacity exhausted), and a waiting VM that breaks is reaped
and replaced. `startup_pool` must be below `max_vms` and leave room for one VM in
`max_memory_mib`. Shutdown and drain reap every waiting VM. `GET /state` on the
internal listener reports `applicationVms`: the VM capacity in use and
`startupPool` (the size, waiting and restoring VMs, pool hits and misses, and
each template's state: `ready`, `building` or `failed` with the seconds until its
retry).

Start the node as usual and add the flag:

```sh
sudo env CELLD_BUCKET=s3://BUCKET/FLEET celld --application-vms /etc/celld/vms.json ...
```

(see the main README for listener and fleet flags). A node with no legacy Worker
deployment serves only application routes; hosts outside the platform domain keep
the original Worker behavior. Routes are served from public ingress, and peers
reach application objects through the original authenticated peer tunnels.

## Package a JavaScript application

Handler source is compiled JavaScript (TypeScript must be compiled first, for
example with `tsc` or a bundler, to ES modules). Build the artifact, a
read-only EROFS image of the source tree:

```sh
python3 -B crates/applications/guest/build-application-image.py ./handler-source ./handler.img
celld application artifact --app shop --file handler.img
```

The builder refuses symlinks, special files, more than 4096 files, an image
over 64 MiB or an existing output. The node caches the image by digest, hard-links
it into the VM's jail and points the template's `handler` drive at it when the VM
is assigned; the guest mounts it read-only on `/application` and loads the
handler from there, so no handler code sits in guest memory beyond the pages it
reads. The `artifact` command prints the SHA-256 of the image; that is the
handler's `artifact_sha256`. Identity is the digest of the uploaded bytes;
fixed filesystem metadata makes repeated builds reproducible for the same files
and erofs-utils. Artifacts are immutable per application. Incompatible images,
wrong block sizes and truncated images are refused before a VM boots.

`entrypoint` is an artifact-relative path such as `main.mjs`. Declared sibling
modules go in `worker_config.modules`:

```json
{
  "compatibility_date": "2026-03-01",
  "compatibility_flags": ["js_rpc"],
  "modules": [
    {"name":"helper.mjs","path":"lib/helper.mjs","kind":"es_module"},
    {"name":"./message.txt","path":"message.txt","kind":"text"},
    {"name":"empty.wasm","path":"empty.wasm","kind":"wasm"}
  ]
}
```

`path` is an artifact file; `name` is its specifier (ES and Wasm modules also
register `./` aliases; text names are verbatim). Paths cannot escape the artifact
or contain symlinks, names cannot shadow builtins, JavaScript and text must be
UTF-8, and at most 4095 sibling modules and 64 MiB of source load. Compatibility
dates and flags use the original machinery. `kind` is `es_module`, `text`, `wasm`
or `data` (binary content imported as an `ArrayBuffer`). A module may add
`"shared": true` to load from the node's shared runtime drive instead of the
image; that drive exists only for a handler whose `worker_config` has `"pyodide"`
(the Python Workers below). Unknown `worker_config` keys refuse launch rather
than being ignored.

## Package a Python application

A Python handler is a JavaScript handler at run time: upstream's Python Workers
(Pyodide 0.28.3, CPython 3.13) are built on the publishing machine into an image
holding one JS file (the handler's code and the Workers SDK), and the guest runs
it in V8 like any other image. The interpreter is not in the image: the node
serves it from one shared read-only drive (below). The guest image has no Python,
and the node never runs esbuild. The manifest says
`"language": "javascript"` (the guest runtime) with `"entrypoint": "index.js"`.

The project is a Wrangler project as upstream's `celld deploy` takes it: `main`
is the `.py` entry, `compatibility_flags` has `python_workers`, the date is
2026-04-21 or later and before 2026-09-08, and `python_modules/` holds the
vendored Workers SDK (`uv run pywrangler sync`; see `examples/python`). Bindings
come from the manifest, not from the project config. `esbuild` must be on `PATH`
(or `CELLD_ESBUILD`; tested with esbuild 0.28.2):

```sh
celld application python --app shop --file ./project/wrangler.jsonc
```

The command builds the project, makes a read-only image of the one JS file,
uploads it as an artifact (digest-addressed like any other) and prints
`artifact_sha256`, `entrypoint` and the `worker_config` to
put in the handler: `pyodide` (the release the node must serve), `modules`
naming the interpreter files the handler loads from the shared drive, plus the
project's compatibility date and flags. On first use the build downloads five Pyodide
0.28.3 files (about 12 MB) from `cdn.jsdelivr.net/pyodide/v0.28.3/full/`; each
file's size and SHA-256 are pinned in the source and a mismatch is refused. They
are cached in `CELLD_PYTHON_RUNTIME_DIR` (default `~/.cache/celld/pyodide-0.28.3`),
which also lets a build run offline.

Supported is what upstream supports: `Default(WorkerEntrypoint).fetch`, bindings
through `self.env`, `waitUntil`, pure-Python packages. Durable Objects,
Workflows, Cron and Queue handlers, containers, compiled (`.so`) wheels and
`process.exit()` are not available in Python; the build refuses the first five
and `.so` wheels.

The shared Pyodide drive is one read-only EROFS per Pyodide release and
runtime image, holding the pinned interpreter files and a
base memory snapshot of a booted interpreter (before any handler import). It is
tenant-agnostic and keyed only by the release and the runtime image. The node
builds it at node startup for every approved runtime, from the pinned,
hash-verified Pyodide files (downloaded, or from `CELLD_PYTHON_RUNTIME_DIR`, as
for the CLI) and a snapshot builder that runs in a VM of its own with no handler
code; it keeps the drive in its private pool directory, hard-links it into every
Python VM's jail as the `shared` drive and mounts it read-only on `/pyodide`.
The node needs `mkfs.erofs` (erofs-utils, `/usr/bin`) for this. A failed build
keeps the node unready; requests never trigger a build or retry.
Each restored VM reseeds Python's `random` from fresh entropy.

Limits and cost:
- The handler image is small (the 64 MiB artifact limit is unchanged). The
  shared drive is about 33.5 MB (12.6 MB of interpreter, 20.9 MB of snapshot)
  and is shared by every Python VM.
- A Python VM loads the interpreter's modules when the guest starts and restores
  a snapshot on its first request instead of booting Pyodide, so its first
  request is slower than a JavaScript handler's; later requests are fast.
- A larger application (pure-Python packages, data) can select a larger named
  class with `limits.size_class`.
- A fatal interpreter error fails its request, and the guest then asks the host
  to retire the VM: the pool leases it no more, reaps it once its events drain,
  and the next request starts a fresh VM. State in Python memory is gone with
  it, as with any VM retirement.
- Compiled-Wasm caches and per-handler snapshots are not implemented.

## Services and `worker_config`

A binding has two parts: a manifest `bindings` alias that points at a registered
resource (or another handler), and, for most kinds, a matching declaration in
`worker_config` that supplies the original class/binding metadata. The two must
agree; a missing, duplicate or unbound declaration refuses launch. Resources are
registered per application, so the same names in two applications never alias.

| Kind (resource) | Manifest binding | `worker_config` |
|---|---|---|
| Durable Objects (`durable_object`) | `"COUNTER": {"kind":"resource","resource":{"kind":"durable_object","id":"counter"}}` | `{"do_classes":["Counter"], "durable_objects":{"bindings":[{"name":"COUNTER","class_name":"Counter"}]}}`. A single declared class may omit `durable_objects`. Normalized `bindings:[{"type":"durable_object_namespace","name":"COUNTER","class_name":"Counter"}]` is also accepted. Classes and bound namespaces must come together. No `do_classes` and no objects is a stateless handler. |
| D1 (`d1`) | resource binding | none |
| KV (`kv`) | resource binding | none |
| Queues, producer (`queue`) | resource binding; the resource id is the queue name | none |
| Queues, consumer | `triggers:[{"kind":"queue","queue":"jobs","handler":"web"}]` plus a registered `queue` resource | optional `queue_consumers:[{"queue":"jobs","max_batch_size":10,"max_batch_timeout":5,"max_retries":3,"dead_letter_queue":"dlq","max_concurrency":2,"retry_delay":10}]` (first four required, the rest optional); without an entry the original defaults apply. Queue and dead-letter queue must be registered queue resources of the same application. Concurrency is capped by what one VM can run. |
| Cron (`cron`) | none (not bindable) | trigger `{"kind":"cron","id":"tick","expression":"* * * * *","handler":"web"}` plus a registered `cron` resource with the same id. The handler's `scheduled(controller, env, ctx)` runs. |
| Service bindings | `"BACKEND": {"kind":"handler","handler":"backend"}` | `"services":[{"binding":"BACKEND","service":"backend","entrypoint":"Named"}]` (`entrypoint` optional). Calls fetch and RPC to another handler of the same deployment. |
| R2 (`r2`) | resource binding; resource id = actual bucket ID; same ID shares objects | `"r2_buckets":[{"binding":"BLOBS","bucket_name":"blobs"}]` (or `bindings:[{"type":"r2_bucket","name":"BLOBS","bucket_name":"blobs"}]`) |
| Workflows (`workflow`) | resource binding; resource id = workflow name | `"workflows":[{"binding":"FLOW","name":"flow","class_name":"Flow"}]` (or `bindings:[{"type":"workflow","name":"FLOW","workflow_name":"flow","class_name":"Flow"}]`); `script_name`, if given, must be the handler's own id. The class extends `WorkflowEntrypoint`. |
| Static assets (`static_assets`) | exactly one resource binding | `"assets": {index, sha256, file_count, total_bytes}`, printed by `celld application assets --directory DIR [--file project.json]`. `project.json` may carry `assets` (`html_handling`, `not_found_handling`, `run_worker_first`), `compatibility_date` and flags. Ingress serves GET/HEAD assets before the handler without a VM unless `run_worker_first` applies; `env.ASSETS.fetch` is brokered to the same resolver. Activation verifies the index and every body. |
| Containers (`container`) | resource binding | `"containers":[{"binding":"BOX","class_name":"Box"}]`; `Box` must be in `do_classes` and bound to a Durable Object namespace. |
| Dynamic Worker loaders (`worker_loader`) | resource binding | `"worker_loaders":[{"binding":"LOADER"}]` (binding name only). Experimental: see limits. |

A Container resource's `configuration` is the only source of its image and
privileged settings: `{"image":"celld-image:<64 hex>","instance_type":"basic","max_instances":N,"runtime":"runsc"}`.
`instance_type`, `max_instances` and `runtime` are optional. Allowed
`instance_type` values are `lite`, `dev`, `basic`, `standard` and
`standard-1` through `standard-4`. The image
must already exist in the fleet bucket at `deploy/images/<key>.tar`, as saved by
the original `celld deploy` of a container class (see
[services/containers.md](services/containers.md)); there is no management or CLI
image upload. The trusted fence image installs the bridge firewall once per
node; without `container_fence_image` or with an unreachable daemon no container
starts.

Environment values (`environment`) are public configuration; `secrets` aliases
are resolved privately at launch (see management guide).

## End to end

```sh
# once per node/fleet: build, image, approve, start nodes (above)
celld application-api --auth-file auth.json --platform-domain apps.example.com --bucket "$CELLD_BUCKET" &
export CELLD_APPLICATION_API=http://127.0.0.1:8090 CELLD_APPLICATION_TOKEN=...

celld application create   --app shop
celld application resource --app shop --file counter.json   # durable_object "counter"
celld application artifact --app shop --file handler.img   # note artifact_sha256
celld application stage    --app shop --file deployment.json
celld application activate --app shop --deployment v1
celld application get      --app shop                       # runtime shows serving; hostnames lists <id>.apps.example.com
curl -H 'Host: <id>.apps.example.com' http://NODE_PUBLIC/...
```

To replace, upload the new artifact, stage `v2` and activate it. Nodes resolve
HTTP routes and cell ownership through exact indexes and current per-application
metadata. They reconcile resident and preparing deployments and retire the old
VMs once their live leases settle; dormant applications need no VM or task.
Requests already running on the old VM finish there (Durable Object data and
the one-writer fence are independent of deployment versions); a request resolved
just before the swap can fail with "deployment is retiring". Roll back by
activating `v1` again; deactivation withdraws the routes and reaps the VMs.

## Drain and recovery

- **Node drain** (graceful stop): public admission closes, in-flight work is
  drained, and the application pool is shut down and reaped before the
  durability seal, so no VM can launch after drain begins. A failed or unproved
  reap prevents a clean-reload certificate; the node then recovers through
  the normal bucket path on restart. Each node removes only its own status
  record.
- **VM loss or crash:** the supervisor reaps the jailer/VMM, native holds are
  released only after the physical reap, and the next event starts a
  fresh VM for the same pinned deployment. Accepted writes are covered by the
  original native proof, not by guest messages.
- **Owner loss:** objects, D1, KV and queues recover on a new owner from
  follower disks or the bucket through the original ownership, sealing and
  takeover. Durable alarms survive eviction, restart and owner loss, and a
  rolled-back alarm installation never fires. Queue messages are redelivered
  after a lost consumer; missed cron ticks fire late with the original retry
  backoff; Workflows replay from their durable state.
- **Quarantined capacity** (an unproved reap) is released only by restarting the
  node.

## Limits and unsupported behavior

Unsupported behavior fails clearly rather than degrading silently.

- **Current coverage:** WebSockets, peer-tunnel sockets and hibernation,
  Durable Object facets, and replacement/drain/rebalance are implemented.
  Dynamic Worker loaders remain
  experimental: the child is a stateless in-guest isolate with no storage and
  no egress. Tail workers, outbound interception and env service bindings from
  loaded workers are refused.
- **Entrypoints:** `fetch`, `queue`, `scheduled`, alarms and RPC are connected;
  other entrypoint kinds are not.
- **Languages:** JavaScript is the handler contract; TypeScript is compiled to
  JavaScript. The CLI's Python Workers packaging path builds a JavaScript
  artifact with the Workers SDK and Pyodide; arbitrary other handler languages
  are unsupported.
- **Event deadlines:** a guest event has a 35 s deadline; very large uploads
  count against it, and the host may clone a request body before a local
  attempt (memory up to a few times the body). Guest `fetch()` outbound request
  bodies must be buffered. Streaming request and response bodies, and large
  Durable Object RPC and D1 arguments, are credited and bounded (64 KiB items).
- **RPC abort signals:** a live `AbortSignal` is relayed for service-binding
  RPC calls only for the life of the brokered fetch; live signals in RPC results,
  Durable Object RPC (which may cross VMs or nodes), facets and loaders refuse
  with `DataCloneError`. A Durable Object RPC `AbortSignal` cancels only the
  caller's wait.
- **Assets:** rolling fallback adoption is unsupported. Static-only applications
  use top-level `assets` and a host-assigned static route.
- **Containers:** images are operator-preloaded; no management upload. Raw TCP
  and port fetch are brokered through the host.
- **Queues:** the original fleet `celld queue` commands do not address
  application queue resources.
- **KV:** expiry follows the original harness, including the 60 s minimum
  `expirationTtl`.
- **Alarms:** a non-awaited `ctx.storage.setAlarm` inside `transactionSync` runs
  after the synchronous transaction ends, as in native Celld.
- **Cron:** minute resolution, UTC, as in the original.
- **Activation checks:** activation does not verify runtime-image approval or
  service-binding configuration (the management process has no node config);
  those fail at first dispatch and appear as `failed` in `runtime`.
- **Status:** a crashed node's last record persists, and handlers not yet
  launched have no entry.

