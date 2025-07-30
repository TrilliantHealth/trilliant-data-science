import inspect
import typing as ty

from ..core.types import Args, F, Kwargs
from .types import FutureShim, Shim, ShimBuilder


class _static_shim_builder:
    def __init__(self, shim: ty.Union[Shim, FutureShim]) -> None:
        self.shim = shim

    def __call__(self, _f: F, _args: Args, _kwargs: Kwargs) -> ty.Union[Shim, FutureShim]:
        return self.shim

    def __repr__(self) -> str:
        return f"<static_shim_builder for {self.shim}>"


def make_builder(shim_or_builder: ty.Union[Shim, ShimBuilder, FutureShim]) -> ShimBuilder:
    """If you have a Shim and you want to make it into the simplest possible ShimBuilder."""

    if len(inspect.signature(shim_or_builder).parameters) == 3:
        return ty.cast(ShimBuilder, shim_or_builder)

    return _static_shim_builder(ty.cast(Shim, shim_or_builder))
