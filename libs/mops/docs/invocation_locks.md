# Invocation Locks/Leases

This feature is currently experimental and may be removed or tweaked as we observe its use.

No configuration or code changes are necessary to use this feature. It is baked directly into the
`MemoizingPicklingRunner`.

Throughout this documentation, we will refer to these as "locks" even though they're more properly
leases, since they have built-in expiration dates and therefore do not ever require human intervention to
recover from failed states.

## Goals

- Allow orchestrators to resume their previously-started remote invocations after exiting
- Prevent separate orchestrators from starting remote invocations if one already has started.

It is not specifically a goal of this feature to provide global-to-the-team Directed Acyclic Runner
capabilities, but it is a practical outcome of the implementation. In other words, it should be possible
to have the same program or multiple programs arbitrarily call functions and allow them to coordinate on
invoking those that are not yet running, or awaiting the results of those that do.

> A failure in this system should never affect the _correctness_ of results; only the efficiency with
> which we use remote resources. Multiple invocations should continue to return equivalent results no
> matter how many times the function is invoked with the same arguments.

## Basic operation

The orchestrator creates/acquires an expiring lock after checking for an existing
[memoized](./memoization.md) result, but prior to passing control to the Shell (which starts the remote
execution). If the lock cannot be acquired (because it is currently held by another process), then
instead of calling the Shell, it will enter a loop which sleeps, then checks for an existing result, and
finally attempts to acquire the lock again.

If the result is ever found prior to acquiring the lock, the orchestrator returns it. If the lock is ever
acquired, the Shell will be called to start a remote invocation.

The lock should be maintained by the acquirer to prevent expiration. In practice, this involves spawning
a separate thread to 'update' the lock's `updated_at` timestamp on a regular schedule. If this timestamp
is not updated for longer than the expiration time, the lock is considered to have expired without
release, and the next attempt to acquire it will succeed.

The `MemoizingPicklingRunner` passes the lock writer id to the remote. On the remote side, early in
startup, the remote will identify the current lock writer id, and only if it matches will it continue to
execute. This allows a form of "last writer wins" to break ties in cases where the acquirer did not
maintain the lock (perhaps due to failure of the orchestrator process).

The remote process will maintain the lock as well once it has started. This way, if the orchestrator
itself dies, then the lock will not expire until after the remote side exits (whether successfully or
unsuccessfully).

When a Shell exits (whether successfully or unsuccessfully), the orchestrator will _release_ the lock
immediately. This is mostly useful as a debugging indication that the orchestrator continued running
until Shell exit, since (in successful cases) the presence of a `result` payload will mean that no other
orchestrator will ever even attempt to acquire the lock.

## Known Limitations

### Delayed remote maintenance + dying orchestrators

With Kubernetes and Databricks, it is possible to have a large delay between acquiring the lock/beginning
the invocation, and the remote function actually beginning to run, because it takes time for nodes and
clusters to spin up.

In this situation, if the orchestrator dies before the remote begins executing, the lock expiration will
eventually arrive, due to lack of lock maintenance. Then any other orchestrator, including a restart of
the original one, will be able to acquire the lock, despite there existing a 'pending' execution of the
function invocation.

The remote runners are configured to exit with a `LockWasStolenError` if, upon startup, they discover
that some other acquirer has acquired the lock before they were able to start.

In theory, this gives us "last orchestrator wins" semantics, which will generally be what we want as far
as the user experience goes, but it will tend to slightly delay completion of a given function, since it
will often be the first remote runner that chooses to exit even though it had been allocated runtime on a
cluster. Since its orchestrator is probably dead and no longer polling for its completion, this is not a
major issue; the later orchestrator will simply have to wait until its remote runner makes it to the head
of the cluster queue on its own terms.

### azure Python SDK network errors

If you're running a huge pipeline, like Demand Forecast, it may be advantageous to opt out of the
network-hungry lock maintenance on the orchestrator side of things. This will tend to create the above
situation more frequently, but the lock should still basically prevent multiple invocations from actually
beginning their computation.

The relevant configuration is a core.config item, so you can programmatically disable it by importing
`MAINTAIN_LOCKS` from `thds.mops.pure.runner.local` and calling `.set_global(False)` on it. You can also
set `THDS_MOPS_PURE_ORCHESTRATOR_MAINTAIN_LOCKS=0` in your environment.
