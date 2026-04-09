import typing as ty

import numpy as np
from sklearn import feature_selection as skfs
from sklearn.base import BaseEstimator, clone
from sklearn.utils import validation as sk_validation

from thds.core.log import getLogger

from .. import array_compat, validation
from ..types import Any1DArray, Any2DArray
from . import univariate


class DynamicFeatureSelection(BaseEstimator, skfs.SelectorMixin):
    def __init__(
        self,
        feature_selector: skfs.SelectorMixin,
        feature_score: univariate.FeatureSelectionScore,
        regression: bool = False,
        min_features: ty.Union[int, float] = 1,
        max_features: ty.Union[int, float, None] = None,
    ):
        """Dynamic feature selection to meet a target range number of features. Useful for achieving practical results
        in high-dimensional datasets, without forcing a model to use an arbitrary fixed number of features.

        How it works:
        First, constant features are filtered out, then a primary feature selector is applied to the remaining features.
        If the resulting number of features lies between `min_features` and `max_features` (inclusive), the process is
        complete. Otherwise, a fixed-size feature selector is applied to achieve a number of features in the target
        range, using the `feature_score` scoring function. In case there were too few features selected by the primary
        selected, _additional_ features are selected from the remaining non-constant features. In case too many features
        were selected, the fixed-size feature selector is applied to the already-selected features to filter them further.

        :param feature_selector: the primary feature selector to use initially
        :param feature_score: the scoring function to use for the final auxiliary feature selection, if needed
        :param regression: whether this is a regression problem or not
        :param min_features: the minimum number of features to select; as an int, it represents an absolute number of
          features, as a float, it represents a proportion of the total number of input features
        :param max_features: the maximum number of features to select; as an int, it represents an absolute number of
          features, as a float, it represents a proportion of the total number of input features. If `None`, no maximum
          is enforced.
        """
        for var, name in (min_features, "min_features"), (max_features, "max_features"):
            if var is not None:
                validation.validate_frac_or_size(var, name)

        self.min_features = min_features
        self.max_features = 1.0 if max_features is None else max_features
        self.feature_selector: skfs.SelectorMixin = clone(feature_selector)  # type: ignore
        self.feature_score = feature_score
        self.regression = regression

    def __sklearn_clone__(self):
        return DynamicFeatureSelection(
            feature_selector=clone(self.feature_selector),  # type: ignore
            feature_score=self.feature_score,
            regression=self.regression,
            min_features=self.min_features,
            max_features=self.max_features,
        )

    def fit(self, X: Any2DArray, y: Any1DArray):
        logger = getLogger(__name__)
        min_features = (
            self.min_features
            if isinstance(self.min_features, int)
            else int(self.min_features * X.shape[1])
        )
        max_features = (
            self.max_features
            if isinstance(self.max_features, int)
            else int(self.max_features * X.shape[1])
        )
        self.feature_names_in_ = sk_validation._get_feature_names(X)
        self.n_features_in_ = X.shape[1]
        self.non_constant_selector_: skfs.SelectorMixin | None = None
        self.feature_selector_: skfs.SelectorMixin | None = None
        self.final_feature_selector_: skfs.SelectorMixin | None = None
        self.aux_feature_selector_: skfs.SelectorMixin | None = None

        logger.info("Filtering constant features")
        non_constant_filter = skfs.VarianceThreshold(0.0)
        non_constant_filter.fit(X, y)
        non_constant_support = non_constant_filter.get_support()
        n_non_constant_features = int(np.sum(non_constant_support))
        if n_non_constant_features <= self.min_features:
            logger.warning(
                "Only %d features are non-constant, less than or equal to min_features (%d); "
                "No further feature selection will be performed",
                n_non_constant_features,
                self.min_features,
            )
            self.non_constant_selector_ = non_constant_filter
            mask = non_constant_support
        else:
            logger.info(
                "%d features are non-constant; performing further feature selection on the remainder",
                n_non_constant_features,
            )
            if n_non_constant_features < X.shape[1]:
                # only need to keep this if it actually filtered anything
                self.non_constant_selector_ = non_constant_filter
                mask = np.copy(non_constant_support)
                X = non_constant_filter.transform(X)
            else:
                mask = np.ones(X.shape[1], dtype="bool")

            fs = clone(self.feature_selector)
            logger.info(
                "Performing feature selection on %d remaining features with %s",
                X.shape[1],
                fs,
            )
            fs.fit(X, y)
            fs_support = fs.get_support()
            n_fs_features = int(np.sum(fs_support))
            if n_fs_features < min_features:
                select_remaining: ty.Optional[int] = min_features - n_fs_features
                logger.warning(
                    "Only %d features selected by main feature selector, less than min_features (%d); "
                    "selecting %d additional features",
                    n_fs_features,
                    min_features,
                    select_remaining,
                )
                if n_fs_features == 0 or n_fs_features >= X.shape[1]:
                    self.feature_selector_ = None
                    # optimization: no need to keep this if it selects all or nothing, and we'll continue down the pipeline
                    # with further feature selection from those that have already been selected
                    select_remaining = None
                    # select_remaining = None means we're not selecting from the _remaining_ features but from those
                    # that have already been selected
                    X_next = X
                elif n_fs_features < X.shape[1]:
                    self.feature_selector_ = fs
                    mask[mask] &= fs_support
                    # now we select from previously un-selected features, to get above min_features
                    X_next = array_compat.slice_2d(X, None, ~fs_support)
                # no need for an else condition because (n_fs_features >= X.shape[1]) or (n_fs_features < X.shape[1])
            elif n_fs_features > max_features:
                select_remaining = None
                # select_remaining = None means we're not selecting from the _remaining_ features but from those
                # that have already been selected
                logger.info(
                    "%d features selected by main feature selector, more than max_features (%d); "
                    "performing further feature selection on the remainder",
                    n_fs_features,
                    max_features,
                )
                if n_fs_features < X.shape[1]:
                    self.feature_selector_ = fs
                    mask[mask] &= fs_support
                    X_next = self.feature_selector_.transform(X)
                else:
                    self.feature_selector_ = None
                    # optimization: no need to keep this if it selects everything, and we'll continue down the pipeline
                    # with further feature selection from those that have already been selected
                    X_next = X
            else:
                # Nothing more to do; we have an acceptable number of features
                logger.info(
                    "Selected %d features out of %d initial non-constant features",
                    n_fs_features,
                    X.shape[1],
                )
                self.feature_selector_ = fs
                mask[mask] &= fs_support
                X_next = None
                select_remaining = None

            if X_next is not None:
                n_to_select = min(select_remaining or max_features, X_next.shape[1])
                # if select_remaining is None, we are selecting from the already-selected features,
                aux_feature_selector = univariate.construct_fixed_size_univariate_fs(
                    self.feature_score, n_to_select, self.regression
                )
                kind = "remaining unselected" if select_remaining else "already selected"
                logger.info(
                    "Performing final feature selection of %d out of %d %s features with %s",
                    n_to_select,
                    X_next.shape[1],
                    kind,
                    aux_feature_selector,
                )
                aux_feature_selector.fit(X_next, y)
                final_support = aux_feature_selector.get_support()
                n_final_features = int(np.sum(final_support))
                if select_remaining is None:
                    # we're selecting from the already-selected features
                    self.final_feature_selector_ = aux_feature_selector
                    mask[mask] &= final_support
                else:
                    # we're selecting from the remaining features
                    self.aux_feature_selector_ = aux_feature_selector
                    # add to the support *outside* the already-selected feature set
                    fs_support[~fs_support] |= final_support
                    mask[non_constant_support] |= fs_support
                    n_final_features = min_features
                logger.info(
                    "Selected %d features out of %d initial non-constant features",
                    n_final_features,
                    n_non_constant_features,
                )
                assert int(mask.sum()) == n_final_features

        self.support_mask_ = mask
        return self

    def _get_support_mask(self) -> np.ndarray:
        """Part of sklearn's SelectorMixin interface; implementing this ensures that get_support() and transform(X)
        work as expected."""
        return self.support_mask_
