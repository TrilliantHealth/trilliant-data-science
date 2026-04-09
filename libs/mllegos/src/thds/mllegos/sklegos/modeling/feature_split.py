from typing import Dict, Hashable, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin, clone
from sklearn.utils.multiclass import unique_labels
from sklearn.utils.validation import check_is_fitted, check_X_y

from thds.core.log import getLogger

from .. import util as sklegos_util
from ..types import Any1DArray, AnyEstimator

_LOGGER = getLogger(__name__)


class DiscreteFeatureSplit(BaseEstimator, TransformerMixin):
    classifier: TransformerMixin
    feature_name: str
    min_frequency: int

    def __init__(self, feature_name: str, classifier: AnyEstimator, min_frequency: int = 1):
        """Learn separate models (transformers/classifiers) for each unique value of a given feature.

        :param feature: the feature to split the dataset on. For each unique value of this feature,
          a separate instance of the `classifier` will be fit on the subset of the data with that value
        :param classifier: an instance of an sklearn transformer or classifier. It will be cloned for
          each unique value of `feature_name`
        :param min_frequency: the minimum number of rows with a given unique value of `feature_name` to
          train a separate instance of `classifier` for. Any values with fewer than this many rows will
          be grouped together into a separate data set for which a fallback instance of `classifier` will
          be trained. In inference, this will be used for any value of `feature_name` which did not get
          its own distinct classfier
        """
        clf_params = {f"classifier__{name}": value for name, value in classifier.get_params().items()}
        self.classifier = classifier
        self.feature_name = feature_name
        self.min_frequency = min_frequency
        self._estimator_type = getattr(classifier, "_estimator_type", None)
        self.set_params(feature_name=feature_name, min_frequency=min_frequency, **clf_params)
        self.value_specific_estimators_: Dict[Hashable, AnyEstimator] = {}
        self.fallback_model_ = clone(classifier)

    def _split_feature(self, X: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        feature_col = X[self.feature_name]
        return feature_col, feature_col.value_counts(dropna=False)

    def _translate_probs(self, probs, clf):
        new = np.zeros((probs.shape[0], len(self.classes_)), dtype=probs.dtype)
        new[:, [self.class_to_ix[c] for c in clf.classes_.tolist()]] = probs
        return new

    def _set_classes(self, y: Any1DArray):
        self.classes_ = unique_labels(y)
        _LOGGER.info(f"Classes are set to {self.classes_}")
        self.class_to_ix = dict(zip(self.classes_.tolist(), range(len(self.classes_))))

    def _set_stats(self, split_feature: Any1DArray, y: Any1DArray):
        # split value on index, class value on columns, observed counts in cells
        self.data_stats_ = (
            pd.DataFrame(dict(split=split_feature, y=y)).value_counts().unstack(fill_value=0)
        )

    def fit(
        self,
        X: pd.DataFrame,
        y: Any1DArray,
        sample_weight: Optional[Any1DArray] = None,
        check_input: bool = False,
    ):
        feature_col, value_counts = self._split_feature(X)
        _LOGGER.info(f"Found {len(value_counts)} distinct values of {self.feature_name}")
        # sklearn boilerplate
        if check_input:
            assert not feature_col.isna().any()
            X, y = check_X_y(X, y, dtype=None, copy=False)

        self._set_classes(y)
        self.value_specific_estimators_.clear()
        low_cardinality_values = value_counts[value_counts < self.min_frequency].to_dict()
        for v, count in value_counts[value_counts >= self.min_frequency].items():
            _LOGGER.info(f"Fitting classifier for subset {self.feature_name}=={v!r} with {count} rows")
            subset = feature_col == v
            try:
                clf = sklegos_util.fit(self.classifier, X, y, subset, sample_weight)
            except ValueError as e:
                # in case all features are pruned by feature selection
                _LOGGER.warning(
                    f"Encountered exception while training model for {self.feature_name} value {v!r}: "
                    f"{e!r}. Model for this value will be trained with other low-cardinality values"
                )
                low_cardinality_values[v] = count
            else:
                self.value_specific_estimators_[v] = clf

        self.low_cardinality_values_ = pd.Series(low_cardinality_values).sort_values(ascending=False)
        _LOGGER.info(
            f"Not fitting separate classifiers for values of {self.feature_name} with fewer than "
            f"{self.min_frequency} rows:\n{self.low_cardinality_values_}",
        )

        low_cardinality_subset = (
            feature_col.isin(low_cardinality_values) if low_cardinality_values else None
        )
        subset_msg = (
            f"{value_counts[self.low_cardinality_values_.index].sum()} rows encompassing {len(low_cardinality_values)} "  # type: ignore
            f"distinct values of {self.feature_name}"
            if low_cardinality_values
            else f"10% of the entire dataset of {X.shape[0]} rows"
        )

        # train on 10% of the data
        if low_cardinality_subset is None:
            low_cardinality_ids = X[[]].sample(frac=0.1).index
            low_cardinality_subset = pd.Series(True, index=low_cardinality_ids).reindex(
                X.index, fill_value=False
            )

        _LOGGER.info(f"Fitting fallback classifier on {subset_msg}")
        self.fallback_model_ = sklegos_util.fit(
            self.classifier, X, y, low_cardinality_subset, sample_weight
        )

        return self

    def _apply_method(self, X: pd.DataFrame, method_name: str) -> np.ndarray:
        # sklearn boilerplate
        check_is_fitted(self)
        feature_col, value_counts = self._split_feature(X)
        subsets = {v: feature_col == v for v in value_counts.index}
        outputs = {
            v: getattr(self.value_specific_estimators_.get(v, self.fallback_model_), method_name)(
                X[subset]
            )
            for v, subset in subsets.items()
        }
        _example_output = next(iter(outputs.values()))
        output_shape = X.shape[0] if _example_output.ndim == 1 else (X.shape[0], len(self.classes_))
        output = np.empty(output_shape, dtype=_example_output.dtype)  # type: ignore
        if method_name == "predict":
            for v, subset in subsets.items():
                output[subset] = outputs[v]
        else:
            for v, subset in subsets.items():
                output[subset] = self._translate_probs(
                    outputs[v],
                    self.value_specific_estimators_.get(v, self.fallback_model_),
                )
        return output

    def predict(self, X: pd.DataFrame) -> pd.Series:
        return pd.Series(self._apply_method(X, "predict"), index=X.index)

    def predict_proba(self, X: pd.DataFrame):
        return pd.DataFrame(self._apply_method(X, "predict_proba"), index=X.index, columns=self.classes_)

    def transform(self, X: pd.DataFrame):
        for method_name in "transform", "predict_proba", "decision_function":
            if hasattr(self.classifier, method_name):
                break
        else:
            raise ValueError(f"Can't transfomr with estimator of type {type(self.classifier)}")
        return pd.DataFrame(
            self._apply_method(X, method_name),
            index=X.index,
            columns=self.classes_ if method_name == "predict_proba" else None,
        )
