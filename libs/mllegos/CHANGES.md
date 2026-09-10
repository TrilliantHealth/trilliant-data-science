# Changes

## 1.3

- Add parquet and JSON `Source` helpers to `io`: `to_parquet_source` (DataFrame or Series; a Series is
  written as a one-column frame), `to_json_source`, and `load_json_source`. Same `out_dir` contract as
  `to_pickle_source`; the module still imports without pandas. New optional extra `parquet` (pyarrow),
  required only to write parquet.
- Add `sklegos.feature_extraction.sparse`: `to_sparse` / `to_dense` conversion functions (safe inside
  pickled `FunctionTransformer` steps) and the `DenseToSparseNamedTransformer`, `SparseCountFeatures`,
  and `SparseCountFeatureNormalized` transformers for count-valued struct columns.
- Add `sklegos.encoding`: `ContinuousFeatureConf` / `OneHotEncoderConf` frozen dataclasses (hashable,
  picklable, safe as memoization-key components), the `ContinuousScaling` / `OneHotUnknownHandling`
  literals, and the `scaling_transformer` / `one_hot_encoder` / `log1p_float` builders that map confs
  onto unfitted sklearn transformers. Encoder assembly (dtype casts, imputation, pipeline order) stays
  with the caller.
- Add `sklegos.eval.binary_cls`: plain-value binary classification scoring - `confusion_counts` /
  `ConfusionCounts`, `binary_cls_scores` / `BinaryClsScores` (accuracy, precision, recall, F-beta family,
  average precision) for 0/1-encoded arrays, and `cls_scores` for arrays of any label dtype with a
  `pos_label`, plus the `frame_cls_scores` / `score_by_variable` conveniences for pandas frames.

## 1.2

- New `eval.viz.cls` module: `confusion_matrix_heatmap`, a pyecharts heatmap for binary confusion
  matrices (behind the existing `pyecharts` extra), with a `ConfusionMatrix` TypedDict for the
  tp/fp/fn/tn counts.

## 1.1

- Add `io`: `to_pickle_source` / `load_pickle_source` / `load_pickle_source_typed` - typed pickle \<->
  `thds.core.source.Source` helpers, free of ML-framework imports.
  `load_pickle_source_typed(*types, src=...)` takes one or more types; a single type infers exactly,
  several infer their common supertype (never `Any`), so a wrong annotation at the callsite cannot pass
  vacuously.
- Add `search_space`: `IntRange` / `FloatRange` frozen dataclasses and `build_skopt_space`, which
  materializes them into `skopt.space` dimensions. New optional extra `skopt` (scikit-optimize).
- Add `sklegos.io`: `dump_model` / `load_model` for pickled `sklearn`-compatible estimators.
