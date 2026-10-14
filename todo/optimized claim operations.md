## Idea: S3 Ownership Coordinator for Faster Cell Claims

**Goal:** Reduce cross-region cell claim latency in Celld while remaining fully compatible with standard S3 APIs.

### Proposed architecture

Allow Celld nodes to delegate ownership resolution and acquisition to designated Celld nodes geographically close to the S3 bucket.

Instead of performing multiple sequential S3 requests across regions:

1. A Celld node sends one RPC to a nearby-to-bucket coordinator.
2. The coordinator reads the current ownership record.
3. It determines whether the cell can be claimed, using Celld's existing ownership and recovery rules.
4. If eligible, it performs an S3 conditional PUT to acquire ownership on behalf of the requesting node.
5. It returns the result, including the ownership epoch or existing owner information.

The coordinator is **not the cell owner**. The requesting node becomes the actual owner and runs the cell.

### Key requirements

- Fully S3-compatible; no R2-specific APIs.
- S3 remains authoritative for ownership and fencing.
- Preserve existing ownership, lease, epoch, and dead-owner recovery guarantees.
- Support multiple coordinators without creating a single point of failure.
- Fall back to direct S3 access if coordinators are unavailable.
- Reuse Celld's existing peer-to-peer communication where appropriate.
- Keep most new logic separate from upstream Celld files to minimize future merge conflicts.

### Potential additional optimization

The coordinator could attempt an optimistic conditional PUT for genuinely new cells, falling back to reading the ownership record when the object already exists.

### Investigation requested

Review the current Celld implementation and produce a concrete TODO/implementation plan covering:

- Current cell claim and ownership resolution flow.
- Existing peer RPC infrastructure that could be reused.
- Best integration points for coordinator delegation.
- Safety considerations, including concurrent claims, ambiguous writes, fencing, and recovery.
- Coordinator discovery, selection, and fallback.
- Expected latency improvements versus direct S3 access.
- Necessary code changes, tests, and benchmarks.

**Important:** This is an optimization of the existing ownership protocol, not a replacement. First determine whether it can be implemented safely with minimal changes to Celld's core logic.

---

## Investigation findings

### Recommendation

**A coordinator can be added safely, but it should initially be an ownership-I/O proxy, not a second owner-decision engine.** Keep `celld-logic` authoritative for the decision to follow a live owner, wait for dead-owner log recovery, select the next epoch, and issue a claim. Delegate only the resulting ownership-store operation, using the same guard and outcome contract as direct S3 access. Duplicating the decision logic in a coordinator would create a second implementation of lease and recovery rules and is not a minimal safe change.

There is one especially small optimization worth measuring first: try `If-None-Match: *` at epoch 1 for a truly absent owner key, and read the record only if that conditional create loses. Released or previously owned cells must still read their record to preserve the monotonically increasing epoch.

### Current claim and resolution flow

1. A request enters the actor and `celld-logic` starts owner resolution (`crates/logic/lib.rs`, `owner_read`). The effect runner in `crates/celld/actor.rs` calls `BucketOwnership::read_owner`, which GETs `cells/<cell>/own.json` from the configured bucket.
2. If the record names this node, the core advances the writer epoch and conditionally acquires against the record's ETag. If the record is released, it advances the saved epoch and does the same. If the key is absent, it proposes epoch 1 with an absent-key guard.
3. If another node owns the cell, the core reads that node's lease. A live, compatible lease routes the request to that owner. An expired or missing lease permits takeover; if the dead owner's folded node log is not sealed, the core first recovers it. The subsequent CAS still uses the original owner record's ETag.
4. The core checks local drain/capacity gates before starting a claim. On success, it records ownership and starts restore (or remains dormant for an ownership-only handoff).
5. `BucketOwnership::cas_owner` writes `cells/<cell>/own.json` with `If-Match` for an existing record or `If-None-Match: *` for an absent one. S3's conditional write remains the cross-node serialization point.
6. A rejected CAS restarts resolution. An ambiguous CAS triggers a readback; only a record with this node and the proposed epoch confirms the claim. Otherwise the core resolves again. It does not infer success from a timeout.

### Peer RPC that can be reused

- `crates/celld/main.rs` routes authenticated internal endpoints, including `/peer/handoff`, `/peer/probe`, and `/peer/log/*`.
- Peer requests use `PeerAuth` fleet HMAC signatures, target-node validation, a replay cache, and protocol-version checks (`crates/celld/peer_auth.rs`). The existing `reqwest::Client` is shared by actor peer operations.
- `/peer/handoff` demonstrates a signed request/response that waits for the target actor's claim before acknowledging. It is a useful transport pattern, but its purpose is transferring a released cell to the receiving peer; it does not provide a generic claim coordinator.
- Node leases already advertise peer address and protocol compatibility. They do not currently advertise a coordinator role, bucket affinity, or bucket-region distance. The configured node region is available locally but is not part of the capacity-peer selection record.

### Minimal integration plan

**Phase 0 — Measure direct claims.** Add no coordinator yet. Record claim latency by existing route stages (`OwnershipRead`, `NodeLeaseLookup`, `OwnershipAcquire`) and fresh versus existing ownership. Use this as the baseline and determine whether the dominant wait is S3 RTT, recovery, routing, or local capacity.

**Phase 1 — Optimistic create for a missing owner key.** In the ownership effect path, attempt the existing absent-key conditional PUT with epoch 1 before GET. On a proven conditional conflict, read the existing record and continue through the unchanged core resolution path. Treat transport errors as ambiguous and read back before retrying. This is only a candidate until tests prove that all callers preserve the correct fresh/takeover metadata and error classification.

**Phase 2 — Optional coordinator transport.** Add a small coordinator client/server module outside `celld-logic`. Have the normal core continue to emit `ReadOwner` and `CasOwner` effects. Route those storage operations through an eligible coordinator when configured, passing the requester identity, cell, epoch, and exact guard; the coordinator executes the same standard-S3 GET or conditional PUT and returns the existing typed result. The requester remains the `node` in the owner record. If selection, peer authentication, or transport fails before a write, use direct S3. If the conditional PUT may have committed, preserve `Ambiguous` and use the existing owner/epoch readback flow rather than retrying a potentially stale guard.

This per-operation proxy is the least invasive first coordinator, but it only helps when the requester-to-coordinator plus coordinator-to-S3 path is faster than requester-to-S3. A single RPC that performs read, policy decisions, and acquisition would save more round trips, but should be considered only after policy is factored into shared code callable by both paths; otherwise it duplicates the core's correctness logic.

### Safety requirements for implementation

- **Concurrent claims:** Every write must retain the original ETag or absent-key condition. Two coordinators may race; at most one conditional PUT wins. A coordinator must never turn a stale read into an unconditional write.
- **Ambiguous writes:** Timeouts and lost responses after PUT are ambiguous. Return that classification unchanged; requester readback of matching node and epoch confirms success. Do not report a definite failure or blindly repeat the write.
- **Fencing and epochs:** Preserve the core-generated epoch and owner identity in the written body. Do not accept an epoch computed independently by the coordinator. Existing writes and cleanup remain fenced by the S3 ownership record and epoch.
- **Owner liveness and recovery:** The core must retain lease validation, peer compatibility checks, drain/capacity checks, and node-log recovery-before-takeover. A coordinator must not claim from a stale lease snapshot or bypass recovery.
- **Identity and authority:** Authenticate the peer request using the existing signed protocol, bind the claimed owner to the authenticated requester, validate cell scope and protocol version, and never expose bucket credentials or arbitrary S3 operations over the peer route.
- **Fallback:** Direct S3 fallback is safe for reads and known-not-committed writes. After an ambiguous write, reconcile the owner record first; a second path must not race a write whose result is unknown.
- **Coordinator failure:** Coordinators are optional accelerators. Select among multiple eligible peers and fall back when none is healthy. S3 remains the authority, so no coordinator-local lock or cache may grant ownership.

### Discovery, selection, and expected latency

The existing lease/capacity records can supply authenticated peer addresses and compatibility information, but they lack coordinator designation and bucket proximity. Start with explicit operator configuration listing coordinator node IDs; resolve IDs through current node leases, and avoid adding a new placement protocol until measurements show it is needed. Choose a healthy compatible coordinator deterministically or by lowest measured RTT, with bounded retries and direct-S3 fallback. Do not use bucket credentials from arbitrary peers.

For an existing/released record, direct cold acquisition generally has at least an owner GET and a conditional PUT, plus a node-lease GET for another owner's record; dead-owner recovery adds more work. A nearby coordinator helps only when the additional peer hop is outweighed by reduced requester-to-bucket latency. If direct requester-to-bucket RTT is `R`, requester-to-coordinator RTT is `P`, and coordinator-to-bucket RTT is `B`, proxying an individual S3 operation is beneficial only when `P + B < R`. An atomic coordinator round trip that combines safe reads and writes could reduce multiple long-haul waits, but requires shared decision logic and more careful ambiguity handling. The absent-key optimistic create can remove one S3 RTT on the genuinely new-cell path without a coordinator.

### TODO: implementation and verification

1. **Baseline:** Capture p50/p95 claim stage timings for new cells, released cells, live-owner routing, dead-owner takeover, and ambiguous-CAS reconciliation across same-region and cross-region buckets.
2. **Optimistic create prototype:** Add the epoch-1 conditional create before owner GET; keep all conflict/recovery decisions in the current core. Add tests for empty bucket, concurrent first claims, a released record, transport ambiguity after commit, and a precondition failure.
3. **Coordinator prototype:** Implement an authenticated ownership RPC in a separate module. Initially support only guarded owner reads/writes, pass the exact `CasGuard`, preserve `CasOutcome`/failure classes, and add explicit configuration for eligible coordinator node IDs. Keep peer protocol compatibility explicit.
4. **Safety tests:** Exercise two requesters racing through different coordinators, stale ETags, coordinator death before and after S3 commit, lost RPC responses, fallback after definite pre-write failure, live-owner lease routing, and dead-owner recovery gating. Confirm the final S3 owner record has the requesting node and strictly advanced epoch.
5. **Benchmarks and rollout:** Compare direct S3, optimistic create, and coordinator paths under measured regional RTTs. Report p50/p95 latency, S3 request count, peer request count, and failure/reconciliation rate. Enable coordinator use only where it improves tail latency without increasing claim failures.

**Scope to keep out of the first change:** regional auto-discovery, coordinator-side copies of ownership policy, cached ownership authority, and any provider-specific conditional-write API. They are unnecessary to test the central latency hypothesis.
