"""The magic sauce for most of what pure.magic does."""

import contextlib
import functools
import typing as ty

from typing_extensions import ParamSpec

from thds.core import stack_context
from thds.mops._utils import config_tree

from ..core import file_blob_store, pipeline_id, pipeline_id_mask, uris
from ..core.memo.unique_name_for_function import full_name_and_callable
from ..core.use_runner import use_runner
from ..pickling.mprunner import MemoizingPicklingRunner
from ..runner.shim_builder import make_builder
from ..runner.simple_shims import samethread_shim
from ..runner.types import Shim, ShimBuilder
from .shims import ShimName, ShimOrBuilder, to_shim_builder

_local_root = lambda: f"file://{file_blob_store.MOPS_ROOT()}"  # noqa: E731
P = ParamSpec("P")
R = ty.TypeVar("R")


class _MagicConfig:
    def __init__(self) -> None:
        # these ConfigTree objects apply configuration to callables wrapped with pure.magic
        # based on the fully-qualified path to the callable, e.g. foo.bar.baz.my_func
        self.shim_bld = config_tree.ConfigTree[ty.Optional[ShimBuilder]](
            "mops.pure.magic.shim", parse=to_shim_builder  # type: ignore
        )
        self.blob_root = config_tree.ConfigTree[ty.Callable[[], str]](
            "mops.pure.magic.blob_root", parse=uris.to_lazy_uri
        )
        self.pipeline_id = config_tree.ConfigTree[str]("mops.pure.magic.pipeline_id")
        self.blob_root[""] = _local_root  # default Blob Store
        self.shim_bld[""] = make_builder(samethread_shim)  # default Shim
        self.pipeline_id[""] = "magic"  # default pipeline_id

    def __repr__(self) -> str:
        return f"MagicConfig(shim_bld={self.shim_bld}, blob_root={self.blob_root}, pipeline_id={self.pipeline_id})"


def new_config() -> _MagicConfig:
    return _MagicConfig()


class Magic(ty.Generic[P, R]):
    """Magic adds mops' powers (memoization, coordination, remote execution) to a callable.

    If you want to _change_ which runtime shim the function is using, that can be set globally
    to the program with pure.magic.shim(other_shim, my_magic_func), and it can also be set
    as a stack-local variable in a context manager provided by this object:

    with my_magic_func.shim("subprocess"):
        my_magic_func(1, 2, 3)

    You can completely disable mops magic for a function in the same ways, either with a contextmanager
    or globally, using `off()`, like so:

    with my_magic_func.off():
        ...
        my_magic_func(1, 2, 3)
    """

    def __init__(
        self,
        func: ty.Callable[P, R],
        config: _MagicConfig,
        calls: ty.Collection[ty.Callable] = frozenset(),
    ):
        functools.update_wrapper(self, func)
        self._func_config_path = full_name_and_callable(func)[0].replace("--", ".")

        self.config = config
        if p_id := pipeline_id_mask.extract_from_docstr(func, require=False):
            # this allows the docstring pipeline id to become 'the most specific' config.
            self.config.pipeline_id.setv(p_id, self._func_config_path)
        self._shim = stack_context.StackContext[ty.Union[None, ShimName, ShimOrBuilder]](
            str(func) + "_SHIM", None  # none means nothing has been set stack-local
        )
        self.runner = MemoizingPicklingRunner(self._shimbuilder, self._get_blob_root)
        self.runner.calls(func, *calls)
        self._func = use_runner(self.runner, self._is_off)(func)
        self.__doc__ = f"{func.__doc__}\n\nMagic class info:\n{self.__class__.__doc__}"
        self.__wrapped__ = func

    @contextlib.contextmanager
    def shim(self, shim_or_builder: ty.Union[None, ShimName, ShimOrBuilder]) -> ty.Iterator[None]:
        """If None is passed, no change will be made."""
        with self._shim.set(shim_or_builder or self._shim()):
            yield

    @contextlib.contextmanager
    def off(self) -> ty.Iterator[None]:
        """off is an API for setting the shim to None,
        effectively turning off mops for the wrapped function.
        """
        with self.shim("off"):
            yield

    @property
    def _shim_builder_or_off(self) -> ty.Optional[ShimBuilder]:
        if stack_local_shim := self._shim():
            return to_shim_builder(stack_local_shim)
        return self.config.shim_bld.getv(self._func_config_path)

    def _is_off(self) -> bool:
        return self._shim_builder_or_off is None

    def _shimbuilder(self, f: ty.Callable[P, R], args: P.args, kwargs: P.kwargs) -> Shim:
        # this can be set using a stack-local context, or set globally as specifically
        # or generally as the user needs. We prefer stack local over everything else.
        sb = self._shim_builder_or_off
        assert sb is not None, "This should have been handled by use_runner(self._off)"
        return sb(f, args, kwargs)

    def _get_blob_root(self) -> str:
        return self.config.blob_root.getv(self._func_config_path)()

    @property
    def _pipeline_id(self) -> str:
        return self.config.pipeline_id.getv(self._func_config_path)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """This is the wrapped function."""
        with pipeline_id.set_pipeline_id_for_stack(self._pipeline_id):
            return self._func(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            f"Magic('{self._func_config_path}', shim={self._shim_builder_or_off},"
            f" blob_root='{self._get_blob_root()}', pipeline_id='{self._pipeline_id}')"
        )


def make_magic(
    config: _MagicConfig,
    shim_or_builder: ty.Union[ShimName, ShimOrBuilder, None],
    blob_root: uris.UriResolvable,
    pipeline_id: str,
    calls: ty.Collection[ty.Callable],
) -> ty.Callable[[ty.Callable[P, R]], Magic[P, R]]:
    def deco(func: ty.Callable[P, R]) -> Magic[P, R]:
        fully_qualified_name = full_name_and_callable(func)[0].replace("--", ".")
        if shim_or_builder is not None:
            config.shim_bld[fully_qualified_name] = to_shim_builder(shim_or_builder)
        if blob_root:  # could be empty string
            config.blob_root[fully_qualified_name] = uris.to_lazy_uri(blob_root)
        if pipeline_id:  # could be empty string
            config.pipeline_id[fully_qualified_name] = pipeline_id
        return Magic(func, config, calls)

    return deco
