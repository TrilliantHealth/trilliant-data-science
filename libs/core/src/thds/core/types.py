import os
import typing as ty
from types import TracebackType

StrOrPath = ty.Union[
    str, os.PathLike
]  # DEPRECATED - please be explicit about this bc it isn't much extra typing


T_Ord = ty.TypeVar("T_Ord", bound="Ord")


class Ord(ty.Protocol):
    """The minimal interface needed to satisfy most common functions relying on orderability, including builtins
    `min`, `max`, and `sorted`."""

    def __lt__(self: T_Ord, other: T_Ord) -> bool:
        pass


_T_co = ty.TypeVar("_T_co", covariant=True)


class ContextManager(ty.Protocol[_T_co]):
    def __enter__(self) -> _T_co: ...

    def __exit__(
        self,
        exc_type: ty.Optional[ty.Type[BaseException]],
        exc_value: ty.Optional[BaseException],
        traceback: ty.Optional[TracebackType],
    ) -> ty.Optional[bool]: ...
