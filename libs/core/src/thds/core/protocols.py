import typing as ty
from types import TracebackType

_T_co = ty.TypeVar("_T_co", covariant=True)


class ContextManager(ty.Protocol[_T_co]):
    def __enter__(self) -> _T_co:
        ...

    def __exit__(
        self,
        exc_type: ty.Optional[ty.Type[BaseException]],
        exc_value: ty.Optional[BaseException],
        traceback: ty.Optional[TracebackType],
    ) -> ty.Optional[bool]:
        ...
