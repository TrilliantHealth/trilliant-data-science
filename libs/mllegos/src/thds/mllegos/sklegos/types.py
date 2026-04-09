import typing as ty

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

try:
    # not a required dependency
    from scipy import sparse as sp

    AnySparseArray = ty.Union[sp.csr_matrix, sp.csr_matrix, sp.coo_matrix]
except ImportError:
    sp = None


AnyEstimator = ty.Union[BaseEstimator, TransformerMixin]
Any1DArray = ty.Union[pd.Series, np.ndarray]
Any2DArray = ty.Union[pd.DataFrame, np.ndarray, "AnySparseArray"]
