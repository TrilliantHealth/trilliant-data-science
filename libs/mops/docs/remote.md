## What do we mean by remote?

A 'remote' runtime at the simplest level is just a different process. `subprocess.run` allows you to
spawn a separate process that will do some work that you require, and, optionally, to wait for its
results.

More commonly, however, a remote runtime is a whole separate computer with its own computing resources.
If we can offload some of the work to be done to a different computer, this is _distributed parallelism_
\- we're no longer limited to getting results as quickly as our [orchestrator](./orchestrator.md) could
produce them on its own, and can instead produce results at a rate equivalent to how parallel our overall
task is.

Commonly, there will be [multiple instances](./orchestrator.md#concurrency) of the remote environment, to
afford greater parallelism.

Transferring execution to a remote environment requires [serialization](./serialization.md) and may have
performance implications, so we have developed some [optimizations](./optimizations.md) that you may want
to be aware of.
