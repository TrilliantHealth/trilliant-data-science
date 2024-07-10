# Invocation Locks

This feature is currently experimental and may be removed or tweaked as we observe its use.

No configuration or code changes are necessary to use this feature. It is baked directly into the
`MemoizingPicklingRunner`.

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

The expiring lock must be maintained by the acquirer. In practice, this involves spawning a separate
thread to 'update' the lock's `updated_at` timestamp on a regular schedule. If this timestamp is not
updated for longer than the expiration time, the lock is considered to have expired without release, and
the next attempt to acquire it will succeed.

On the remote side, early in startup, the remote will begin to 'accompany' the orchestrator in
maintaining the expiring lock. This way, if the orchestrator itself dies, then the lock will not expire
until after the remote side exits (whether successfully or unsuccessfully).

When a Shell exits (whether successfully or unsuccessfully), the orchestrator will _release_ the lock
immediately. This is mostly useful as a debugging indication that the orchestrator continued running
until Shell exit, since (in successful cases) the presence of a `result` payload will mean that no other
orchestrator will ever even attempt to acquire the lock.

## Known Limitations

### Delayed remote maintenance + dying orchestrators

If a very large pipeline (such as Demand Forecast) is run, it will create more k8s Jobs than there are
nodes immediately available to run them. In this situation, if the orchestrator dies before the remote
begins executing, the lock expiration will eventually arrive, due to lack of lock maintenance. Then any
other orchestrator, including a restart of the original one, will be able to acquire the lock, despite
there existing a 'pending' execution of the function invocation.

This limitation can be helped somewhat by increasing lock expirations to larger numbers. This is not
currently configurable within `mops` but could easily be made so by using `core.config.item`.

An alternative, and more complete, solution, would be to pass the `lock_uuid` out-of-band to the remote
side (via shell arguments), and ask it to exit quietly if it found that the existing lock was active with
a different id than expected. This would allow 'stealing acquirers' to start their own invocation, with
reasonable confidence that any long-delayed invocations would exit prior to actually invoking the
function.
