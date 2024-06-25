import typing as ty

T = ty.TypeVar("T")


def null_safe_iter(it: ty.Optional[ty.Iterable[T]]) -> ty.Iterator[T]:
    """Iterate the iterable if it is not None"""
    if it is not None:
        yield from it
