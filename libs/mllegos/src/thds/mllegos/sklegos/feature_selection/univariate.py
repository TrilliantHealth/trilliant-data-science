import typing as ty
from functools import partial

from sklearn import feature_selection as skfs

from thds.core.log import getLogger

from .. import validation

FeatureSelectionCriterion = ty.Literal["FDR", "FPR", "FWE"]
FeatureSelectionScore = ty.Literal["F", "chi2", "MI"]
_AnySelector = ty.Union[skfs.SelectFdr, skfs.SelectFpr, skfs.SelectFwe]

_LOGGER = getLogger(__name__)
_CRITERION_TO_SELECTOR: ty.Mapping[FeatureSelectionCriterion, ty.Type[_AnySelector]] = {
    "FDR": skfs.SelectFdr,
    "FPR": skfs.SelectFpr,
    "FWE": skfs.SelectFwe,
}
_NAME_TO_SCORE_FUNC_CLF: ty.Mapping[FeatureSelectionScore, ty.Callable] = {
    "F": skfs.f_classif,
    "chi2": skfs.chi2,
    "MI": skfs.mutual_info_classif,
}
_NAME_TO_SCORE_FUNC_REG: ty.Mapping[FeatureSelectionScore, ty.Callable] = {
    "F": skfs.f_regression,
    "chi2": skfs.chi2,
    "MI": skfs.mutual_info_regression,
}


def construct_univariate_fs(
    criterion: FeatureSelectionCriterion,
    score: FeatureSelectionScore,
    alpha: float,
    regression: bool = False,
) -> skfs.SelectorMixin:
    """Construct a statistical univariate feature selector which selects features according to a statistical criterion.

    :param criterion: name of the statistical criterion to use for selection
    :param score: name of score function to use for selection
    :param alpha: the significance level to use for the statistical test. Smaller values indicate a stricter
      criterion for feature selection and fewer features selected
    :param regression: Whether this is for a regression or classification problem; True indicates regression
    """
    cls = _CRITERION_TO_SELECTOR[criterion]
    score_func = (_NAME_TO_SCORE_FUNC_REG if regression else _NAME_TO_SCORE_FUNC_CLF)[score]
    return cls(score_func=score_func, alpha=alpha)


construct_univariate_fs_clf = partial(construct_univariate_fs, regression=False)
construct_univariate_fs_reg = partial(construct_univariate_fs, regression=True)


def construct_fixed_size_univariate_fs(
    score: FeatureSelectionScore,
    max_features: ty.Union[int, float],
    regression: bool = False,
) -> ty.Union[skfs.SelectKBest, skfs.SelectPercentile]:
    """Construct a fixed-size univariate feature selector which selects a fixed proportion or number of features.

    :param score: name of score function to use for selection
    :param max_features: either the total number of features to keep, when an integer, or the proportion of features
      to keep (between 0 and 1), when a float
    :param regression: Whether this is for a regression or classification problem; True indicates regression
    """
    score_func = (_NAME_TO_SCORE_FUNC_REG if regression else _NAME_TO_SCORE_FUNC_CLF)[score]
    validation.validate_frac_or_size(max_features, "max_features")
    if isinstance(max_features, int):
        return skfs.SelectKBest(score_func=score_func, k=max_features)
    elif isinstance(max_features, float):
        return skfs.SelectPercentile(score_func=score_func, percentile=max_features * 100.0)
    else:
        raise TypeError(f"max_features must be an int or float; got {type(max_features)}")


construct_fixed_size_univariate_fs_clf = partial(construct_fixed_size_univariate_fs, regression=False)
construct_fixed_size_univariate_fs_reg = partial(construct_fixed_size_univariate_fs, regression=True)
