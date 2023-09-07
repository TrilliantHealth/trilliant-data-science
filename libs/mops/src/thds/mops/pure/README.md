# thds.mops.pure

A facility for transparently running _pure_ Python functions
in such a way that their execution environment can be independent
of the caller, and so that their results can be memoizable.

The core `MemoizingPicklingRunner` implementation uses:

- `pickle` for serialization
- `ADLS` for remote storage

though each of these implementation details is independent of the
others, and other implementations of the same system could be written.

The standard remote shell implementation is Kubernetes, but integration
tests run using a simple subprocess shell, and running on Azure
Functions or local Docker containers would be easy to implement.
