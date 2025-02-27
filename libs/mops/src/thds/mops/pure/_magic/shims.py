import typing as ty

from thds import core

from ..runner.shim_builder import make_builder
from ..runner.simple_shims import samethread_shim, subprocess_shim
from ..runner.types import Shim, ShimBuilder

ShimName = ty.Literal[
    "samethread",  # memoization and coordination, but run in the same thread as the caller.
    "subprocess",  # memoization and coordination, but transfer to a subprocess rather than remote.
    "off",  # equivalent to None - disables use of mops.
]
ShimOrBuilder = ty.Union[ShimBuilder, Shim]
logger = core.log.getLogger(__name__)


def _shim_name_to_builder(shim_name: ShimName) -> ty.Optional[ShimBuilder]:
    if shim_name == "samethread":
        return make_builder(samethread_shim)
    if shim_name == "subprocess":
        return make_builder(subprocess_shim)
    if shim_name == "off":
        return None
    logger.warning("Unrecognized shim name: %s; mops will be turned off.", shim_name)
    return None


def to_shim_builder(shim: ty.Union[None, ShimName, ShimOrBuilder]) -> ty.Optional[ShimBuilder]:
    if shim is None:
        return None
    if isinstance(shim, str):
        return _shim_name_to_builder(shim)
    return make_builder(shim)
