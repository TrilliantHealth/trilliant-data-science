from typing import Optional, Union

from sklearn.utils import validation


def validate_frac_or_size(
    frac_or_size: Union[int, float], name: str, max_size: Optional[int] = None
) -> None:
    """Validate that var is either a positive integer or a float in (0, 1]. Useful for validating sample size parameters.

    Parameters
    ----------
    frac_or_size : Union[int, float]
        Variable to validate.
    name : str
        Name of the variable for error messages.

    Raises
    ------
    ValueError
        If var is not a positive integer or a float in (0, 1].
    TypeError
        If var is not an int or float.
    """
    if isinstance(frac_or_size, int):
        validation.check_scalar(frac_or_size, name, target_type=int, min_val=1, max_val=max_size)
    elif isinstance(frac_or_size, float):
        validation.check_scalar(frac_or_size, name, target_type=float, min_val=0.0, max_val=1.0)
    else:
        raise TypeError(f"{name} must be an int or float; got {type(frac_or_size)}")
