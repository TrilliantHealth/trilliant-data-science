import numpy as np
import pandas as pd
from packaging import version

PANDAS_VERSION_LT_2_0 = version.parse(pd.__version__) < version.parse("2.0")


def resolve_numeric_np_index_dtype_for_pd_version(dtype: str | np.dtype) -> np.dtype:
    """Resolve the numeric numpy index dtype depending on the installed pandas version."""
    dtype_ = np.dtype(dtype)

    if dtype_.kind not in ("iuf"):
        raise TypeError(
            f"{dtype} is not, or does not resolve to, an (unsigned) integer or float. Resolved to {dtype_}"
        )

    if not PANDAS_VERSION_LT_2_0:
        return dtype_

    if dtype_.kind == "i":
        return np.dtypes.Int64DType()
    elif dtype_.kind == "u":
        return np.dtypes.UInt64DType()
    else:  # already type-narrowed `dtype_` so we know it is a float type at this point
        return np.dtypes.Float64DType()
