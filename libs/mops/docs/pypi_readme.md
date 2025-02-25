`mops` is a Python library for ML Operations.

Jump to
[Quickstart](https://github.com/TrilliantHealth/trilliant-data-science/blob/main/libs/mops/docs/quickstart.adoc)
if you ~~are impatient~~ prefer examples, like me!

`mops` solves for four core design goals:

- [Efficient](https://github.com/TrilliantHealth/trilliant-data-science/blob/main/libs/mops/docs/optimizations.adoc)
  transfer of
  [pure](https://github.com/TrilliantHealth/trilliant-data-science/blob/main/libs/mops/docs/pure_functions.adoc)
  function execution to
  [remote](https://github.com/TrilliantHealth/trilliant-data-science/blob/main/libs/mops/docs/remote.adoc)
  execution environments with more &| different compute resources

- Everything is written in standard Python with basic Python primitives; no frameworks, YAML, DSLs...

- [Memoization](https://github.com/TrilliantHealth/trilliant-data-science/blob/main/libs/mops/docs/memoization.adoc)
  — i.e. _reproducibility and fault tolerance_ — for individual functions.

- Droppability: `mops` shouldn't entangle itself with your code, and you should always be able to run
  your code with or without `mops` in the loop.

It is used by
[decorating or wrapping your pure function and then calling it](https://github.com/TrilliantHealth/trilliant-data-science/blob/main/libs/mops/docs/magic.adoc)
like a normal function.

### read the docs

[Browse our full documentation here.](https://github.com/TrilliantHealth/trilliant-data-science/blob/main/libs/mops/README.adoc)
