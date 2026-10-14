# Fork cleanup: shrink the guest diff against upstream celld

Written 2026-10-13 from a read-only review of the working tree (HEAD = upstream
v0.6.2 plus uncommitted work). Nothing here is implemented. Facts below were read
from source; items marked **[H]** are hypotheses that need a test first.

## Goal

Make future upstream merges cheap by moving Firecracker-guest logic out of
upstream files, without weakening correctness or security. Fewer upstream lines is
not the goal on its own: a change that hides an upstream argument change from the
merger is worse than a visible conflict.

## Why not a central dispatcher

The ~88 guest sites are four different problems. Only the first is "swap an op".

| Class | Sites | Fix |
| --- | --- | --- |
| Whole-op substitution (R2 13, container ~10, some ws, loader/facet/svc/do_call/asset in `js.rs`) | ~40 | Install-time override (step 3) |
| Helper-level backend choice (`storage_ops.rs`, 20 `guest_storage::channel(` sites) | ~28 | Keep the branch; V8 encode/decode is shared with native, so op-level swap would copy it and drift silently |
| Event lifecycle (`has_guest_storage` in `start_cell_event`, `settle`, `cancel`, `finish_turn`) | ~35 hunks | Permanent hooks |
| Plumbing (fields, `context,` threading, `cleanup: None`, `guest_activity::track`, `cfg(unix)`) | ~120 tiny hunks in `js.rs` | Permanent; shrink only where cheap |

Constraints any design must respect:

- Interception must sit inside the `spawn_count()` / `attribute_spawns` window of
  `attributed` in the `ops!` macro (`crates/celld/js.rs`, ~7234). Outside it, a
  guest-enqueued op is adopted by the running turn and the waiting event never
  settles.
- `guest_storage_enabled` is set at construction for root guests (`js.rs` ~6458,
  before `install_ops`). It is only ever stored `true`; the one post-load flip is
  `own_guest_facet` (`js.rs` ~6940) on loaded child workers. Ops are bound as
  parameters of compiled scripts, so a flipped child cannot get new functions.
  Install-time override is therefore valid for root guests only.
- Op names must stay a build-time constant. The bootstrap code cache does not
  cover the parameter list (`js/bootstrap.rs` ~19). Swap function values, never
  names.
- The guest-side check is not the security boundary. The host is: alias binding
  (`application_guest_transport.rs` `resource_target`), `broker_owner`, and
  `output().prove`.

## Steps, in order

### 1. Relocate guest-only code verbatim (do first)

Pure new items with no upstream callers; mechanical, no behaviour change.
Estimated 37 hunks, ~1,800 lines.

- `js.rs`: ~24 hunks, ~1,000 lines (`cell_gate_wait` block, HeapBytes diagnostics,
  rpc-signal mirroring, `open_facet_file`, `prepare_facet`, `register_actor_name`
  additions, `impl Worker` guest methods).
- `js/storage_ops.rs`: 7 hunks, ~340 lines.
- `js/container.rs`: 2 hunks, ~225 lines. `js/websocket.rs`: 2 hunks, ~207 lines.
- `js/r2_ops.rs`: the ~545-line in-file test block (from `check_application_broker`)
  to `unit_tests/` via `#[path]`, as the other tests already do. Move
  `guest_return` and `guest_binding` next to the guest code.
- Guest `impl Worker` / `impl IoContext` blocks go in `js/guest_storage.rs`
  (a child of `js`, so it sees private items).

Done when: build and existing tests pass, and `git diff HEAD --stat` for the
upstream files drops by roughly the figures above.

### 2. Op classification ledger and CI test

New `js/guest_ops.rs` with `CLASS: &[(&str, Class)]`, one entry per op in
`OP_NAMES` and `TEST_OP_NAMES`. Classes: `Pure`, `LocalState`, `Brokered`,
`Prohibited`, `Inline` (has a branch), `Unreviewed`.

- Seed from the 194-op scan (78 have a direct guest mention, 113 do not).
- Test: every op name is classified, and the `Unreviewed` count never rises. A new
  upstream op turns CI red until classified.
- Review the unreviewed suspects: `__kv_blob`, `__queue_*`, `__cron_*`, `__do_id`,
  `__d1_run`, `__log`, `__tcp_*`, `__vfs_*`, `__process_exit`. Record why each is
  safe. `__kv_blob` today fails closed only because the guest process has no native
  cell storage; `__tcp_*` only because of `EgressPolicy::Deny`.
- Prohibited ops get an explicit deny at install time rather than relying on
  absent native channels (`GATE_TX`, `SVC_CALL_TX`, `FACET_TX`, `R2_STORE` are
  unset in `celld-guest`; they are set in in-process tests, which hides
  fall-through).

### 3. Proof of concept: install-time override of `__r2_head`

1. In `ops!`, extract the body of `attributed` into `attributed_op(scope, args, rv,
   name, op)`. The per-op `attributed` still exists and calls it. Behaviour,
   `call_op_patch`, and distinct callbacks unchanged.
2. `guest_ops.rs`: `OVERRIDES: &[(&str, OpFn)]` with `("__r2_head", r2_head)`.
   `r2_head` decodes args 0 and 1, builds `R2Operation::Head`, calls `guest_return`.
   Each override gets a guest wrapper that calls `attributed_op`, so accounting is
   shared, not copied.
3. In `install_ops`, after `install_op_functions`: if the isolate flag is set or
   `guest_activity::enabled()`, replace entries by `OP_NAMES` position. Names and
   code cache untouched. The lazy `internal_arguments` path reads the same
   `internals`, so it stays consistent.
4. Delete the guest branch from `op_r2_head`; it becomes identical to upstream.
5. Checks:
   - Same `bucket.head()` JS in native and guest, compare the JSON.
   - Attribution: enqueue from event A's continuation while event B's microtask
     checkpoint runs; assert the op's owner is A.
   - `__r2_get` and the other unmigrated ops behave as before.
   - Existing `check_application_broker` still passes.
6. Rollback: remove one table entry.

**Stop and decide** after this step. Migrate the other R2 ops and the container
whole-op swaps only if the PoC was clean and the next upstream merge shows the
benefit. Do not extend to storage helpers or lifecycle code.

### 4. Mode-model fix (separate, behaviour-changing)

Do not bundle with steps 1-3.

- **[H]** `EgressPolicy::Deny` means "no network device" for a root guest
  (`js.rs` ~6459) and "capability denied" for a loaded child with
  `globalOutbound: null` (`js.rs` ~8631). `op_fetch` skips the Deny check in a
  guest isolate (`js.rs` ~10403). A child with `globalOutbound: null` promoted by
  `own_guest_facet` may be able to `fetch()` through the host broker. Write a test
  first. If confirmed, split the two meanings.
- **[H]** Loaded children cannot obtain an R2 binding (`r2_bindings` is empty at
  `js.rs` ~8693). Test it, because the override design leans on it.
- Consider constructing loaded children as guest from the start instead of
  flipping later. This changes egress and bootstrap behaviour; needs its own tests.
- Three modes exist (native, host-pool `application_storage`, guest) and three
  predicates (isolate flag, per-event `IoContext.guest_storage`, process-wide
  `guest_activity::ENABLED`). Name them; do not add a fourth.

### 5. Smaller cleanups (as time allows)

- One `asyncrt` hook for `guest_activity::track` on `enqueue`/`spawn` instead of
  per-site wrappers. The raw `tokio::spawn` / `spawn_blocking` sites
  (`js.rs` ~8717, ~9630, ~9785) would stay.
- Make `guest_storage::enabled()` available on non-unix (returns false) to drop
  some of the 39 `#[cfg(unix)]` sites in `js.rs`.
- Native/guest error-delivery differences in R2: `op_r2_delete`, `op_r2_put`,
  `mp_complete` throw synchronously in the guest branch but reject the promise in
  native. Harmless if the harness always awaits inside an async function; confirm
  and align.
- Brokered R2 proves output for reads (head/get/list); native does not. Stricter,
  safe, but not equivalent; document it.

## Unavoidable upstream changes (do not spend time on these)

- Lifecycle hooks in `start_cell_event`, `begin_cell`, `settle`, `cancel`,
  `finish_turn`, plus `InFlight` / `IoContext` fields.
- `load_config_inner(.., guest)`, `WorkerConfig` / `ActorRuntimeState` fields,
  the tiny plumbing hunks.
- Six guest-only entries in the `ops!` table.
- Storage helper hooks and the SQL cursor-id enum.
- `harness.js` passing a trailing `bucketName` to R2 ops.
- `broker_owner` threading in R2 `native_*`. Needed for multi-tenancy: upload ids
  come from one node-global counter. Keep the owner check inside `native_*`.

Upstream churn explains why the lifecycle hunks dominate: `js.rs` changed in 12 of
13 releases, `bootstrap.rs` 10, `websocket.rs` 9, `storage_ops.rs` 6, `r2_ops.rs` 5.
Expect `js.rs` merges to stay manual.

## Expected result (estimates, not measured)

- Hunks across the touched upstream files: 348 now, about 270 after (-22%).
- Added lines in those files: about 45-50% fewer.
- No per-call overhead on native isolates from steps 1-3.

## Open questions

- How often will we actually merge upstream? If rarely, do steps 1-2 and stop.
- Compile the native modules out of `celld-guest` (it is a second binary of the
  same crate and links everything)? Strongest hardening, much larger change;
  separate decision.
- Nothing here was run. Per-op overhead of a flag check was not profiled.
