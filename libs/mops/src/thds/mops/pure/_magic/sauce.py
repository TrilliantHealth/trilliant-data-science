"""The magic sauce for most of what pure.magic does."""

import contextlib
import functools
import typing as ty

from typing_extensions import ParamSpec

from thds.core import futures, log, stack_context
from thds.mops._utils import config_tree

from .. import core
from ..core.memo.unique_name_for_function import full_name_and_callable
from ..core.use_runner import use_runner
from ..pickling.mprunner import MemoizingPicklingRunner
from ..runner.shim_builder import make_builder
from ..runner.simple_shims import samethread_shim
from ..runner.types import Shim, ShimBuilder
from .shims import ShimName, ShimOrBuilder, to_shim_builder

_local_root = lambda: f"file://{core.file_blob_store.MOPS_ROOT()}"  # noqa: E731
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
            "mops.pure.magic.blob_root", parse=core.uris.to_lazy_uri
        )
        self.pipeline_id = config_tree.ConfigTree[str]("mops.pure.magic.pipeline_id")
        self.blob_root[""] = _local_root  # default Blob Store
        self.shim_bld[""] = make_builder(samethread_shim)  # default Shim
        self.pipeline_id[""] = "magic"  # default pipeline_id

        self.all_registered_paths: set[str] = set()

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
        magic_config_path: str,
        calls: ty.Collection[ty.Callable] = frozenset(),
    ):
        functools.update_wrapper(self, func)
        self._magic_config_path = magic_config_path

        self.config = config

        if p_id := core.pipeline_id_mask.extract_from_docstr(func, require=False):
            # this allows the docstring pipeline id to become 'the most specific' config.
            self.config.pipeline_id.setv(p_id, self._magic_config_path)
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
        return self.config.shim_bld.getv(self._magic_config_path)

    def _is_off(self) -> bool:
        return self._shim_builder_or_off is None

    def _shimbuilder(self, f: ty.Callable[P, R], args: P.args, kwargs: P.kwargs) -> Shim:  # type: ignore[valid-type]
        # this can be set using a stack-local context, or set globally as specifically
        # or generally as the user needs. We prefer stack local over everything else.
        sb = self._shim_builder_or_off
        assert sb is not None, "This should have been handled by use_runner(self._off)"
        return sb(f, args, kwargs)

    def _get_blob_root(self) -> str:
        return self.config.blob_root.getv(self._magic_config_path)()

    @property
    def _pipeline_id(self) -> str:
        return self.config.pipeline_id.getv(self._magic_config_path)

    def submit(self, *args: P.args, **kwargs: P.kwargs) -> futures.PFuture[R]:
        """A futures-based interface that doesn't block on the result of the wrapped
        function call, but returns a PFuture once either a result has been found or a a
        new invocation has been started.
        """
        with core.pipeline_id.set_pipeline_id_for_stack(self._pipeline_id):
            return self.runner.submit(self.__wrapped__, *args, **kwargs)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """This is the wrapped function - call this as though it were the function itself."""
        with core.pipeline_id.set_pipeline_id_for_stack(self._pipeline_id):
            return self._func(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            f"Magic('{self._magic_config_path}', shim={self._shim_builder_or_off},"
            f" blob_root='{self._get_blob_root()}', pipeline_id='{self._pipeline_id}')"
        )


def make_magic_config_path(func: ty.Callable) -> str:
    return full_name_and_callable(func)[0].replace("--", ".")


class MagicReregistrationError(ValueError):
    pass


def make_magic(
    config: _MagicConfig,
    shim_or_builder: ty.Union[ShimName, ShimOrBuilder, None],
    blob_root: core.uris.UriResolvable,
    pipeline_id: str,
    calls: ty.Collection[ty.Callable],
    *,
    config_path: str = "",
) -> ty.Callable[[ty.Callable[P, R]], Magic[P, R]]:
    """config_path is a dot-separated path that must be unique throughout your application.

    By default it will be set to the thds.other.module.function_name of the decorated function.
    """
    error_logger = log.auto(__name__, "thds.mops.pure._magic.api").error
    err_msg = (
        "You are probably using pure.magic(.deco) from multiple places on the same function. You will need to specify a unique config_path for each usage."
        if not config_path
        else f"You supplied a config_path ({config_path}) but you reused the decorator on different functions with the same config_path."
    )
    err_msg += " See the comment in mops.pure._magic.sauce for more details."

    def must_not_remagic_same_func(msg: str) -> None:
        error_logger(f"{msg}; {err_msg}")
        # if you see either of the above messages, consider whether you really need the magic
        # configurability of pure.magic, or whether it might be better to instantiate and use
        # MemoizingPicklingRunner directly without configurability. The reason overwriting
        # configs, by applying pure.magic to the same callable from more than one location is
        # disallowed is that you will get 'spooky action at a distance' between different parts
        # of your application that are overwriting the base config for the same function.
        # Another approach would be to use a wrapper `def` with a static @pure.magic decorator
        # on it that calls the inner function, so that they are completely different functions
        # as far as pure.magic is concerned.
        raise MagicReregistrationError(msg)

    magic_config_path_cache: set[str] = set()
    # the reason for this cache is that there are cases where you may want to apply the _exact
    # same_ base config to the same function multiple times - just for ease of use. And
    # since this is the exact same config, we should allow it and treat it as though you
    # had only applied it once.  Of course, if you later try to configure these
    # applications separately, it won't work - these _are_ the same magic config path, so
    # they're bound together via that config.

    def deco(func: ty.Callable[P, R]) -> Magic[P, R]:
        fully_qualified_name = make_magic_config_path(func)
        magic_config_path = config_path or fully_qualified_name

        def deco_being_reapplied_to_same_func() -> bool:
            return fully_qualified_name in magic_config_path_cache

        if magic_config_path in config.all_registered_paths and not deco_being_reapplied_to_same_func():
            must_not_remagic_same_func(f"Cannot re-register {magic_config_path} using pure.magic")

        if shim_or_builder is not None:
            config.shim_bld[magic_config_path] = to_shim_builder(shim_or_builder)
        if blob_root:  # could be empty string
            config.blob_root[magic_config_path] = core.uris.to_lazy_uri(blob_root)
        if pipeline_id:  # could be empty string
            config.pipeline_id[magic_config_path] = pipeline_id

        magic_config_path_cache.add(fully_qualified_name)
        config.all_registered_paths.add(magic_config_path)
        return Magic(func, config, magic_config_path, calls)

    return deco


F = ty.TypeVar("F", bound=ty.Callable)


def wand(
    config: _MagicConfig,
    shim_or_builder: ty.Union[ShimName, ShimOrBuilder, None] = None,
    # None means 'pull from config' - 'off' means off.
    *,
    blob_root: core.uris.UriResolvable = "",
    pipeline_id: str = "",
    calls: ty.Collection[ty.Callable] = tuple(),
) -> ty.Callable[[F], F]:
    """A higher-order function factory that prefers your arguments but falls back to magic
    config at the time of wrapping the function.

    You are creating a magic wand, not doing magic. In fact, the wand doesn't actually use
    Magic at all - it resolves things from config as soon as the function is _wrapped_,
    not at the time of function call.
    """

    def deco_that_resolves_and_locks_in_config(func: F) -> F:
        magic_config_path = make_magic_config_path(func)
        shim_builder = (
            to_shim_builder(shim_or_builder)
            if shim_or_builder
            else config.shim_bld.getv(magic_config_path)
        )
        if not shim_builder:  # this means 'off'
            return func  # just run the function normally

        blob_root_uri = (
            core.uris.to_lazy_uri(blob_root) if blob_root else config.blob_root.getv(magic_config_path)()
        )

        return core.pipeline_id.set_pipeline_id_for_stack(
            pipeline_id or config.pipeline_id.getv(magic_config_path)
        )(use_runner(MemoizingPicklingRunner(shim_builder, blob_root_uri).calls(func, *calls))(func))

    return deco_that_resolves_and_locks_in_config
