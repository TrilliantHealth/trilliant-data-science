# Sometimes you need to 'unwrap' a partial. This will let you do that.
import typing as ty
from functools import partial

from .types import Args, Kwargs, T


def unwrap_partial(
    func: ty.Callable[..., T], args: Args, kwargs: Kwargs
) -> ty.Tuple[ty.Callable[..., T], Args, Kwargs]:
    while isinstance(func, partial):
        args = func.args + tuple(args)
        kwargs = {**func.keywords, **kwargs}
        func = func.func
    return func, args, kwargs
