from typing import Optional, Union

import pandas as pd
from sklearn.base import BaseEstimator, clone

from .types import Any1DArray, Any2DArray


def _select(arr: Union[pd.DataFrame, Any1DArray], selector: Optional[Any1DArray]):
    if selector is None:
        return arr
    elif isinstance(arr, (pd.DataFrame, pd.Series)):
        return arr.loc[selector]
    else:
        return arr[selector]


def fit(
    classifier: BaseEstimator,
    X: Any2DArray,
    y: Any1DArray,
    subset: Optional[Any1DArray],
    sample_weight: Optional[Any1DArray],
    *,
    copy: bool = True,
):
    if copy:
        classifier = clone(classifier)
    kw = (
        dict()
        if sample_weight is None
        else dict(
            sample_weight=_select(sample_weight, subset),
        )
    )
    return classifier.fit(
        _select(X, subset),
        _select(y, subset),
        **kw,
    )
