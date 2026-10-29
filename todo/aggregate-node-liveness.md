# Spike: aggregate node liveness through relays

Status: design only. No code or spec has changed. The TLA+ models must be
updated and checked before any code is written.

## Recommendation

Raise `CELLD_TTL_MS` now. Build aggregation only for fleets that grow past a
few thousand nodes, and start with the TLA+ change.

- **Raising the TTL** (already documented) cuts renewal writes by a constant
  factor. They still grow with the node count.
- **Relay-vouched liveness** makes renewal writes grow with the relay count
  instead. At 10,000 nodes that is about 500 times fewer bucket writes.
- **Write safety does not depend on the lease** (see "What the lease is for"),
  so the change is narrower than it first looks. Only the clock-based part
  changes: when a peer may take a node's cells, and when the node must stop
  serving them. The epoch, CAS and acknowledgement fences are untouched.

## Cost at 10,000 nodes

A node renews every `CELLD_TTL_MS`/3. These are class A writes, at
$4.50 per million on R2.

| Option | Writes per child per hour | Fleet writes per second | Per month |
|---|---|---|---|
| Today, TTL 10 s | 1,080 | 3,000 | 7.8 billion, about $35,000 |
| TTL 30 s | 360 | 1,000 | 2.6 billion, about $11,700 |
| TTL 60 s | 180 | 500 | 1.3 billion, about $5,800 |
| Aggregated, 20 relays, TTL 10 s | about 0 | 6 | 16 million, about $70 |
| Aggregated, 200 relays, TTL 10 s | about 0 | 60 | 160 million, about $700 |

**What each option costs besides writes:**
- **A longer TTL** slows failover. A dead node's cells move only after its
  lease expires: up to 30 s or 60 s instead of 10 s.
- **Aggregation** keeps the TTL short but adds a dependency. A child's authority
  now rests on its relay, or on its fallback to self-renewal.
- **Aggregated rows assume** a child's own record is written only at boot and
  when its node-log state changes (ensemble changes, recovery). Those writes are
  rare and don't depend on the TTL.

**A side benefit:** the elected waker and dead-node GC read `nodes/` with one
GET per node every tick. With aggregation they read one record per relay, so
about 20 GETs instead of 10,000.

## What the lease is for

What the current per-node record, `nodes/<node>.json`, does:

1. **Write safety: not the lease.**
   - Ownership is a CAS on the cell's ownership record, and every activation
     advances the epoch.
   - A stale owner can write only into a superseded epoch prefix.
   - A bucket-proof acknowledgement re-reads the ownership record without
     checking a clock.
   - A fleet-proof acknowledgement is fenced because a takeover seals the old
     node-log session before it restores (`docs/guarantees.md`, "The mechanism").
   - So none of "one writer per epoch", "no lost acknowledged write" or "epoch
     fence" depends on who renews leases or how often.
2. **Takeover timing.** A peer takes a cell over only when the owner's lease has
   expired (`expires_ms <= now`, as in `logic/dead_node_reconciliation.rs` and
   `drain.rs`).
3. **Self-fence and read freshness.**
   - The node checks its published expiry on every routed request and fences
     when it passes.
   - With synchronized clocks, the old owner therefore stops serving before any
     peer may take over. That is what prevents a stale read after a takeover.
   - The node also fences at once when its record is missing or not what it
     wrote (`node_lease_record_mismatch_fence`), which catches a second process
     using the same node name.
4. **Routing.** An ingress caches a remote route until the owner's observed
   lease deadline.
5. **Node-log state.**
   - `log_state` in the record (`NodeLeaseRecord.log_state`) gates a takeover
     behind node-log recovery when a session acknowledged past the bucket.
   - Peers CAS this record to `Recovering` or `Sealed` during takeover recovery.
6. **Identity and discovery:**
   - `addr`, `peer_protocol`, `generation` and the probe key, used for routing,
     signed probes, the waker's push and `celld diagnose`;
   - the load block, used for placement and autoscalers.

Aggregation needs to change items 2, 3 and 4. It must keep 5 and 6 intact.

## Design

### Split the record

- **Each node's own record, `nodes/<node>.json`**, keeps identity, `addr`,
  `generation`, the probe key, `log_state`, and a new field: `via`, the relays it
  heartbeats to.
  - The node writes it with a CAS at session start, as acquisition does today.
    It writes it again only when `log_state` or the ensemble changes.
  - Its `expires_ms` is still honoured, but in steady state it is only the
    fallback (see "Fallback" below).
- **Each relay writes one record, `liveness/<relay>.json`:**
  `{ relay, written_ms, children: { <node>: { generation, expires_ms, load } } }`.
  It writes it every `CELLD_TTL_MS`/3, and only that relay writes it, so there is
  no CAS contention.
- **The published deadline** of node `n` is the largest `expires_ms` naming `n`
  at its current generation, across its own record and every record in its
  `via` list.

### Heartbeat

- **The child:** every `CELLD_TTL_MS`/3 it sends a signed `POST /peer/lease`
  carrying its node name, generation and load block. It sends it to each relay
  it follows, at least the current one and ideally both.
- **The relay:**
  - folds heartbeats into its next record write;
  - sets each child's `expires_ms` to the heartbeat's arrival time plus the TTL;
  - answers each heartbeat only **after** the record holding the new deadline
    has landed;
  - sends the deadline it recorded in that answer.
- **The child's authority** is the latest deadline a relay confirmed to it. It
  fences when that passes, exactly as it does today with its own deadline.

### The rules a model check must prove

| Rule | Statement | Why |
|---|---|---|
| R1 Confirm after write | A child believes a deadline only once a record carrying at least that deadline has landed in the bucket. | A child never believes it has longer than peers can see. |
| R2 Monotone | A relay never lowers a child's recorded deadline. It drops a child's entry only after that deadline has passed. | Shortening or removing a live entry would let peers take over while the child still serves. |
| R3 One generation | A relay vouches for a child only at the generation in that child's own record. It reads the record on the first heartbeat and again when a heartbeat's generation changes. A heartbeat from an older generation gets a `superseded` answer, and that child fences at once. | This replaces `node_lease_record_mismatch_fence`. A same-name second process cannot keep the first one alive. |
| R4 Undecided is not dead | A peer may take over `n` only when every source in `n`'s own record and `via` list was read, and every deadline in them has passed. A source that fails to read leaves `n` undecided. | This matches today's rule that an unreadable lease record is neither live nor dead. |
| R5 Fallback | A child whose relays all fail to confirm writes its own record's `expires_ms` by CAS before its confirmed deadline, with the margin today's renewal timeout already uses. | A relay outage turns back into today's protocol instead of fencing a whole subtree. |

What failures look like:
- **A relay dies:** its children fail over to their other relay within one
  heartbeat interval. Children with no other relay fall back to self-renewal.
  None of them fence.
- **A child dies:** its entries stop advancing. Peers take its cells over after
  the published deadline, as they do today.

### What changes in the ownership fence

- **Unchanged:** epochs, the ownership CAS, the acknowledgement rule, the
  takeover recovery gate and the use of `log_state`.
- **Changed:** two inputs.
  - The deadline a peer compares with `now` before a takeover becomes the
    published deadline: a maximum over several records instead of one field.
  - The deadline a node fences at becomes the latest relay-confirmed one.
- **Routing:** caches use the published deadline.
- **The safety argument for read freshness stays the same,** provided R1 and R2
  hold: the believed deadline is never later than the published one.

## TLA+ work before code

The coordination specs are maintained outside this repository (see
`docs/testing.md`). Code comments name `CelldWriterGeneration.tla`,
`CelldPersistencePaged.tla` and `CelldPagedVfs.tla`.

**Must change:**
- **The coordination, ownership and lease model:** the spec with the invariants
  "one writer per epoch" and "no lost acknowledged write", and with node leases
  as the takeover precondition. It needs:
  - Relay state.
  - New actions: `Heartbeat(n, r)`, `RelayWrite(r)`, `Confirm(r, n)`,
    `RelayCrash(r)`, `ChildFailover(n)`, `SelfRenew(n)` and `DropEntry(r, n)`.
  - The published deadline as a derived value.
- **`CelldWriterGeneration.tla`:** process generations and same-name restarts
  are what R3 governs.

**Unchanged:** `CelldPersistencePaged.tla` and `CelldPagedVfs.tla`. They model
epoch chains and paged files, not liveness.

**New invariants**, next to the existing ones, which must stay as they are:
- `NoStaleServe`: a node that serves a cell has a believed deadline no later than
  its published deadline.
- `MonotonePublished`: a node's published deadline never decreases while that
  node is live.
- `TakeoverAfterExpiry`: a takeover of `n` happens only after its published
  deadline has passed.

**Pinned failing configurations** (each must produce a counterexample):
- **The relay confirms before its write lands:** breaks R1 and leads to a stale
  serve.
- **The relay drops a live entry:** breaks R2 and leads to an early takeover.
- **An unreadable relay record is counted as expired:** breaks R4.
- **A superseded generation is still vouched for:** breaks R3, with two serving
  processes under one name.

## Implementation phases (after the model passes)

1. **Own record:** stop renewing `expires_ms` in the steady state. Keep the CAS
   for session start and `log_state` changes, and add `via`.
2. **Relay side:** add `POST /peer/lease`, batched writes of
   `liveness/<relay>.json`, confirmation after the write, and the R3 generation
   check.
3. **Liveness for peers:**
   - The published deadline in the core's lease reads (`logic` crate: takeover,
     drain and rebalance filters, which use `expires_ms > now` today).
   - Routing caches.
   - The waker and dead-node GC scan, which reads relay records instead of
     `nodes/`.
4. **Child side:** fence on the confirmed deadline, the `superseded` answer, and
   self-renewal as the fallback (R5).
5. **Placement:** read load from relay records.
6. **Rollout:**
   - Mixed-version fleets keep self-renewal until every reader understands
     `via`. Readers ship first, then writers.
   - A node only starts relying on relays once all its peers can read them.

## Open questions

- **Heartbeats on the sync long-poll:** should the heartbeat ride on the
  `/peer/sync` long-poll? Polls are held up to 20 s, longer than a 10 s TTL
  allows, so a separate small request looks necessary.
- **Load block freshness:** the load block is sampled every second today but
  written every renewal. Is TTL/3 freshness through the relay enough for
  autoscalers?
- **Clocks:** deadlines stay wall-clock based, as today. The model keeps its
  perfect-clock assumption. Skew tolerance is unchanged, but it now spans two
  clocks (relay and peer), which is worth a note in `docs/guarantees.md`.
