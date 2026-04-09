from typing import Dict, List, Literal, Mapping, Optional

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from ..types import Any1DArray

ImputeStatistic = Literal["mean", "median", "mode"]


class ConditionalImputer(BaseEstimator, TransformerMixin):
    def __init__(
        self,
        conditional_features: List[str],
        impute_features: Mapping[str, ImputeStatistic],
        min_freq: int = 100,
    ):
        """
        Simple imputation of a set of numeric feature according to an estimate of their values within
        each unique combination of a set of other categorical features.

        :param conditional_features: the features to condition on. These should be discrete and
          low-cardinality. For each unique combination of them, the `statistic` of each of the
          `impute_features` will be computed and used as the imputation estimate
        :param impute_features: mapping from the names of the features to impute to the aggregate
          central tendency statistic used to impute them.
        :param min_freq: the minimum number of observed values of each of the `impute_feature`s to allow
          to serve as an estimate of that feature's within-group central tendency. Unique combinations of
          the `conditional_features` with fewer than this number of nonnull values of the imputed feature
          will not get their own estimate for that feature. Instead, an estimate from a larger set of
          values will be used, either the set of unique combinations of `conditional_features` which
          failed to meet this threshold, or the entire dataset if that set still has fewer than
          `min_freq` nonnull values of the imputed feature.
        """
        self.conditional_features = conditional_features
        self.impute_features = impute_features
        self.min_freq = min_freq
        self.set_params(
            conditional_features=conditional_features,
            impute_features=impute_features,
            min_freq=min_freq,
        )

    @property
    def _agg(self):
        """for use as an argument to `groupby.agg`"""
        return {
            name: (lambda x: x.mode()) if stat == "mode" else stat
            for name, stat in self.impute_features.items()
        }
        # .groupby().agg(stat) doesn't support "mode" as a named stat - a function has to be passed.
        # For the other cases, there are optimized implementations

    def fit(self, X: pd.DataFrame, y: Optional[Any1DArray] = None):
        agg_stats = self._agg
        groups = X.groupby(self.conditional_features)[list(self.impute_features)]
        conditional_stats = groups.agg(agg_stats)
        counts = groups.count()
        self.conditional_stats_ = {
            name: series[counts[name] >= self.min_freq].dropna()
            for name, series in conditional_stats.items()
        }
        low_freq = groups.transform("count") < self.min_freq
        self.fallback_stats_ = {
            name: X.loc[
                low_freq_ixs if low_freq_ixs.sum() >= self.min_freq else slice(None), name  # type: ignore
            ].agg(agg_stats[name])
            for name, low_freq_ixs in low_freq.items()
        }
        return self

    def transform(self, X: pd.DataFrame, y: Optional[Any1DArray] = None):
        new: Dict[str, pd.Series] = dict()
        conditional = (
            pd.MultiIndex.from_frame(X[self.conditional_features])
            if len(self.conditional_features) > 1
            else pd.Index(X[self.conditional_features[0]])
        )
        for colname, stats in self.conditional_stats_.items():
            isna = X[colname].isna()
            if isna.any():
                new_col = X[colname].copy()
                is_known = conditional.isin(stats.index)
                can_impute = isna & is_known
                new_col.loc[can_impute] = stats.loc[conditional[can_impute]].values
                fallback_value = self.fallback_stats_.get(colname)
                new_col.fillna(fallback_value, inplace=True)
                new[colname] = new_col  # type: ignore

        return X.assign(**new) if new else X
