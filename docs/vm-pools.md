# VM Pools

## 1. Overview

Each node manages its own Firecracker VMs. Snapshot templates provide fast starts, an unassigned pool keeps VMs ready for use, and assigned VMs serve repeated requests for the same handler.

```text
Snapshot template
        |
   Restore + prewarm
        |
   Unassigned VM
        |
   One-time claim
        |
   Assigned VM
        |
   Running <-> Paused
        |
   Retire + destroy
```

## 2. Templates and Startup Pool

A **template** is an immutable snapshot of a booted guest before any application is loaded. Templates are specific to a runtime, kernel, VM size and Firecracker configuration. They are prepared before the node accepts traffic and rebuilt after restart.

The **startup pool** holds restored, prewarmed VMs with no application data. Background refill maintains a configured target for templates the node has needed, within available capacity.

If no matching VM is waiting, a request restores one from its template. Disabling the startup pool keeps templates but restores each new VM on demand.

**Boosted startup:** VMs can temporarily use their full vCPU allocation during runtime initialization and application loading, even when their normal CPU quota is fractional. Boost concurrency is bounded by host capacity. Waiting and ready VMs return to their normal quota; startup proceeds without a boost when none is available.

**Generic VM sizing (design goal):** Keep unassigned VMs usable across resource sizes, reducing the need for a separate pool per size class. Today's implementation uses size-specific templates; size-independent pooling remains a future direction.

**Optional approach:** Initialize an application-independent runtime after restore, preserve that process through assignment, and load application code, secrets and bindings only when claimed. This moves runtime initialization off the request path. It is one possible approach for another codebase, not a required implementation. Supporting multiple resource sizes from the same pool also requires a way to adapt CPU and memory allocations; runtime prewarming alone does not provide that.

## 3. Assignment and Reuse

A claim loads an application into an unassigned VM. Each assigned VM belongs to one **application, deployment and handler**. Concurrent requests share that VM, and concurrent starts for the same identity share one startup.

Every restored VM receives fresh randomness before runtime initialization. Once assigned, it never returns to the unassigned pool or serves another identity.

Idle assigned VMs can pause and resume when work arrives. Pausing requires confirmation that both host and guest work have drained. Requests wait for resume within their original deadlines. Active requests, streams and background work prevent idle retirement.

## 4. Capacity and Memory

The node bounds VM count and total reserved guest memory. Starting, waiting, assigned and unreaped VMs all consume capacity.

Pausing alone does not free a reservation. Optional enforced memory caching can reduce a paused VM's physical reservation, but waking it requires reserving its full execution budget again.

When capacity is needed, discard unassigned VMs first, then eligible assigned idle VMs by least recent use. Memory pressure blocks new starts and refill while protecting active work.

## 5. Retirement and Cleanup

Assigned VMs retire after idle expiry or when their deployment is replaced and its work finishes. Shutdown closes admission, stops refill and destroys the remaining VMs.

Each VM has an owner responsible for its entire lifecycle, including failed starts and canceled callers. Capacity is released only after process termination and cleanup are confirmed. If cleanup cannot be proved, that capacity remains reserved in quarantine.
