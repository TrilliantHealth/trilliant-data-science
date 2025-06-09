import inspect
import typing as ty

from ..core.types import Args, F, Kwargs
from .types import Shim, ShimBuilder


class _static_shim_builder:
    def __init__(self, shim: Shim) -> None:
        self.shim = shim

    def __call__(self, _f: F, _args: Args, _kwargs: Kwargs) -> Shim:
        return self.shim

    def __repr__(self) -> str:
        return f"<static_shim_builder for {self.shim}>"


def make_builder(shim: ty.Union[Shim, ShimBuilder]) -> ShimBuilder:
    """If you have a Shim and you want to make it into the simplest possible ShimBuilder."""

    if len(inspect.signature(shim).parameters) == 3:
        return ty.cast(ShimBuilder, shim)

    return _static_shim_builder(ty.cast(Shim, shim))
