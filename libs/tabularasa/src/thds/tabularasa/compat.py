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

    if hasattr(np, "dtypes"):  # 2.x introduces .dtypes
        # NumPy 2.x
        if dtype_.kind == "i":
            return np.dtypes.Int64DType()
        elif dtype_.kind == "u":
            return np.dtypes.UInt64DType()
        return np.dtypes.Float64DType()

    # NumPy 1.x fallback
    if dtype_.kind == "i":
        return np.dtype("int64")
    elif dtype_.kind == "u":
        return np.dtype("uint64")
    return np.dtype("float64")
