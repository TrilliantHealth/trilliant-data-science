# Changes

## 1.1

- Add `io`: `to_pickle_source` / `load_pickle_source` / `load_pickle_source_typed` - typed pickle \<->
  `thds.core.source.Source` helpers, free of ML-framework imports.
  `load_pickle_source_typed(*types, src=...)` takes one or more types; a single type infers exactly,
  several infer their common supertype (never `Any`), so a wrong annotation at the callsite cannot pass
  vacuously.
- Add `search_space`: `IntRange` / `FloatRange` frozen dataclasses and `build_skopt_space`, which
  materializes them into `skopt.space` dimensions. New optional extra `skopt` (scikit-optimize).
- Add `sklegos.io`: `dump_model` / `load_model` for pickled `sklearn`-compatible estimators.
