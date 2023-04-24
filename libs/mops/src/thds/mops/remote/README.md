# thds.mops.remote

A facility for transparently calling Python functions in a remote
process from a local orchestrator process by passing the arguments and
the results to and from the remote process via some kind of Python
object channel.

The current `AdlsPickleRunner` implementation uses:

- `pickle` for serialization
- `ADLS` for remote storage

though each of these implementation details is independent of the
others, and other implementations of the same system could be written.

The standard remote shell implementation is Kubernetes, but integration
tests run using a simple subprocess shell, and running on Azure
Functions or local Docker containers would be easy to implement.

## Philosophy (important!)

This library does provide some benefits that other frameworks will
not, and attempts to make it possible to convert lots of disparate
codebases to use its facilities without invasive changes. In order to
remain relatively simple, however, it requires that you piece together
various pieces and understand how those pieces fit together. Read on
for specifics...

## Usage




### Src/Dest Files
## Result memoization/caching
