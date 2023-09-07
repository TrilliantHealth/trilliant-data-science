# mops

`mops` is a Python library for ML Operations.

It solves for three core issues:

- Transfer of pure function execution to [remote](./remote.md) execution environments
  with more &| different compute resources
- [Efficient](./optimizations.md) transfer of required data to/from other
  environments.
- [Memoization](./memoization.md) — i.e. _reproducibility_ — of individual tasks

It is used by
[decorating or wrapping your pure function and then calling it](./basic_usage.md).

It requires:

- Python
- itself to be installed in the [remote](./remote.md) execution
  context as well as on the [orchestrator](./orchestrator.md).
- ['pure' functions](./pure_functions.md)
- your function and its arguments to be [serializable](./serialization.md) with `pickle`.
- ADLS access on the [orchestrator](./orchestrator.md) and the
  [remote](./remote.md) runtime.

It is usually used with remote compute on:

- [Kubernetes](./kubernetes.md), with code distributed as a Docker image
   <br />---or---
- [Databricks/Spark](../../dbxtend/README.md), with code distributed as Python wheels

It optionally integrates with:

- [`joblib`](https://joblib.readthedocs.io/en/latest/)

It has some [limitations](./limitations.md).
