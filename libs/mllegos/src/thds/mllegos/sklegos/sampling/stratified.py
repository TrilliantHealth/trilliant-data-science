from typing import Optional, Union

import numpy as np
import pandas as pd

from .. import array_compat, validation
from ..types import Any1DArray


def stratified_sample_ixs(
    by: Any1DArray,
    frac_or_size: Union[float, int],
    random_state: Optional[int] = None,
) -> np.ndarray:
    """Select integer array indices for a stratified sample stratified on the values in the given array.
    The sample indices are chosen such that the distribution of values in `by` is approximately preserved in the sample,
    *and* every unique value in `by` is represented in the sample. Note that this latter condition means that when
    `frac_or_size` is an int, the actual number of samples returned may be slightly larger than requested, especially in
    cases where there are many unique values in `by` with small counts. For instance, a request for 10 samples from a
    `by` array with 20 unique values will return at least 20 samples.

    The result of this operation can be used to index into arrays to obtain a stratified sample, using e.g.
    `array_compat.slice_2d`.

    Why do we not just use `sklearn.model_selection.train_test_split`? Because it errors on inclusion of any class with
    only one member, and we want to be able to handle such cases gracefully. It also may omit low-frequency classes
    entirely from the smaller split, which we do not want.

    Parameters
    ----------
    by : Any1DArray
        Array of stratification labels.
    frac_or_size : float or int
        Fraction or total number of samples to return.
    random_state : Optional[int], optional
        Random state for reproducibility, by default None.

    Returns
    -------
    numpy.ndarray
        Array of selected indices.

    Raises
    ------
    ValueError
        If `frac_or_size` is not a positive integer or a float in (0,1], or if it exceeds the length of `by`.
    """
    total_rows = len(by)
    validation.validate_frac_or_size(frac_or_size, "frac_or_size", max_size=total_rows)

    if isinstance(frac_or_size, float):
        total_sample_size = int(np.ceil(frac_or_size * total_rows))
    else:
        total_sample_size = min(frac_or_size, total_rows)

    s = pd.Series(array_compat.to_np(by), copy=False, index=range(len(by)))
    label_counts = s.value_counts().sort_values()
    target_sample_sizes = label_counts * total_sample_size / total_rows
    frac_parts, whole_parts = np.modf(target_sample_sizes)
    sample_sizes = whole_parts.astype(int)
    sample_sizes[sample_sizes == 0] = 1  # ensure every label is represented at least once
    if (remaining := total_sample_size - sample_sizes.sum()) > 0:
        # distribute remaining samples to labels with largest fractional parts
        increment_ixs = np.argpartition(-frac_parts, remaining)[:remaining]
        sample_sizes.iloc[increment_ixs] += 1

    per_label_sample_sizes = sample_sizes.to_dict()

    def sample(group: pd.Series) -> pd.Series:
        return group.sample(
            n=max(per_label_sample_sizes[group.name], 1),
            random_state=random_state,
        )

    sampled = s.groupby(s).apply(sample)
    return sampled.index.get_level_values(-1).values
    # last level of the index is the range index we assigned at the beginning
