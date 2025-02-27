# mops core lock

The purpose of this module is to define locking semantics that work for an orchestrator/remote invocation
pairing.

In particular, the basic requirements are:

1. Exclude other orchestrators on a best-effort basis.
1. Recover from a network partition or other failure (i.e. lock liveness).
1. The remote must be able to maintain lock liveness unconditionally as long as it is running.

The basis for the second requirement is that we can be certain that processes holding the lock will
sometimes die or be terminated. We cannot require human involvement to recover from these scenarios.
Therefore, the lock must be able to 'expire' if not actively kept live.

The basis for the third requirement is more subtle. But we can be equally certain that on some occasions,
the orchestrator process itself will fail or be terminated, while the remote process continues to
progress. Part of the intent of mops is to prevent unnecessary recomputation of results; as of the
implementation of feature, not just those that have already been fully computed, but those that are
currently pending completion. In other words, if your orchestrator process dies and you want to restart
it, you should now be able to restart it immediately, without waiting for running remote invocations to
run to completion.

## Blob Store vs ADLS

We have historically restricted ourselves to a limited number of required underlying 'Blob Store'
capabilities, so that `mops` can remain generic across different underlying third-party systems. While we
have implemented the majority of its functionality against Azure Data Lake Storage (ADLS) in practice,
`mops` should not be an ADLS-only tool, in case the business later finds a need to switch cloud providers
(or, heaven forbid, become multi-cloud).

The above restriction has imposed very few costs in practice so far, but this new coordination
requirement runs up against its disadvantages. ADLS provides certain semantics upon which it would be
reasonably easy to build a 'guaranteed' lock - a lock where "at most one" holder can exist at the same
time. These are the lock semantics we as software developers are most used to.

Notably, ADLS offers the ability to _fail_ an upload if the file already exists - this is a powerful
semantic when the underlying store is consistent, because it provides an inherent coordination mechanism.
If you are the first to write to a named file, you win the race for that lock.

However, this is NOT an affordance provided by some competing blob stores, such as Amazon S3, and we
would prefer not to start making exceptions at this stage for the genericity of our Blob Store
abstraction.

## Two-owner lock

Even if we were willing to break the Blob Store abstraction in favor of a simpler lock, the third
requirement (for there to be two 'cooperative' owners of the same lock (orchestrator and remote)
complicates any simplistic concept of locking. Two processes will both need to be able to update the
lock's state without themselves coordinating - and this requires a different notion of locking than is
easily supported by some off-the-shelf systems for distributed coordination. Even the built-in Locks in
Redis don't provide a natural way to 'share' a lock's liveness updates, once the lock is acquired.

## Approach

Therefore, for now, our implementation is going to be a bespoke algorithm that will hopefully prove
resilient in real-world scenarios, while admittedly not being a bulletproof algorithm. We're going to do
the best we can with the basic Blob Store abstraction, and build something that will work for 99% of use
cases.

The essential concept of the algorithm is:

- generate a local-only UUID representing our attempt to get the lock
- check a known lockfile
- if it exists and it contains our UUID, we have acquired the lock!
- if it exists and it doesn't contain our UUID:
  - then either it has not expired, in which case we return to the beginning and retry
  - or it has expired, in which case we can attempt to acquire it...
- if it does not exist, or if it has expired, we write to the lockfile, with our UUID and a timestamp.
  - we then wait a specified period of time before returning to the beginning to find out whether we
    successfully acquired the lock.

The lock algorithm is therefore "last writer wins", and it is theoretically prone to live-lock, if too
many writers keep attempting to write to the lock. In practice, we hope that it will be both fairly rare
that multiple orchestrators simultaneously attempt to acquire the lock, as well as rare that there are
enough writers that live-lock becomes realistically possible.

The remote-side lock liveness comes in when the remote process begins - it can continually update the
lockfile with a new timestamp without interfering with the orchestrator process - they'll just go back
and forth keeping it up to date, overwriting the timestamp but maintaining the same UUID without checking
to confirm anything.

Only the orchestrator will 'release' the lock directly - when the remote function invocation ends and the
Shim returns, the orchestrator will confirm that a result exists and then release the lock.

If the orchestrator dies and the remote eventually dies as well, the lock will go stale/expire, and that
will allow any future callers, or callers who were waiting on the existing invocation, to acquire the
lock and therefore launch their own invocation.
