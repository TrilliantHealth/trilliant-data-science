import typing as ty

K = ty.TypeVar("K", bound=ty.Hashable)
T = ty.TypeVar("T")

Normalizer = ty.Callable[[T], ty.Optional[T]]
