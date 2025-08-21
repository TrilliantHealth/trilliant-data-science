from functools import singledispatch
from typing import Callable, Optional, Type

import cattrs
import cattrs.errors
import cattrs.v


class DisallowedConversionError(TypeError):
    """Raised when a value is not allowed to be converted to a certain type."""

    def __init__(self, value_type: Type, target_type: Type):
        super().__init__(value_type, target_type)
        self.value_type = value_type
        self.target_type = target_type

    def __str__(self):
        return f"Refusing to structure value of type {self.value_type} to type {self.target_type}"


@singledispatch
def format_cattrs_exception(exc: BaseException, type_: Optional[Type]) -> str:
    """Default formatter for cattrs exceptions. Extensible via `singledispatch` to handle other exception types."""
    return cattrs.v.format_exception(exc, type_)


@format_cattrs_exception.register(DisallowedConversionError)
def _format_disallowed_conversion_error(exc: DisallowedConversionError, type_: Optional[Type]) -> str:
    """Override cattrs' default exception formatting for our more strict approach to type conversion.
    Cattrs is extremely lax - e.g. `None` is permitted for `str` (structures to `"None"`) or `bool`
    (structures to `False`), which we prevent with custom hooks and signal with a custom exception."""
    return f"invalid value for type, expected {exc.target_type.__name__}, got {exc.value_type.__name__}"


@format_cattrs_exception.register(cattrs.errors.ClassValidationError)
def format_cattrs_classval_error(
    exc: cattrs.errors.ClassValidationError,
    format_exception: Callable[[BaseException, Optional[Type]], str] = format_cattrs_exception,
) -> str:
    """Format a cattrs ClassValError into a human-readable string."""

    sep = "\n  "
    return (
        f"{exc.message}:{sep}"
        f"{sep.join(reversed(cattrs.transform_error(exc, format_exception=format_exception)))}"
    )
