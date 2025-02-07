"""Magic is an attempt at a new interface for mops designed to make it even less sticky
and easier to get things done with.

It's designed to combine the most common workflows into a single wrapper function
requiring an absolute minimum of boilerplate/config.

Unlike the more open-ended interface of use_runner plus BYO Runner, this one assumes
MemoizingPicklingRunner, and the most likely non-default config will be a runtime Shim or
ShimBuilder.  If you don't supply one, it will default to the same-thread shim.
"""

import contextlib
import functools
import typing as ty
from pathlib import Path

from typing_extensions import ParamSpec

from thds import core
from thds.mops import config
from thds.mops._utils import config_tree

from .core import file_blob_store, pipeline_id_mask, uris
from .core.memo.unique_name_for_function import full_name_and_callable
from .core.use_runner import use_runner
from .pickling.mprunner import MemoizingPicklingRunner
from .runner import Shim, ShimBuilder
from .runner.shim_builder import make_builder
from .runner.simple_shims import samethread_shim, subprocess_shim

ShimName = ty.Literal[
    "samethread",  # memoization and coordination, but run in the same thread as the caller.
    "subprocess",  # memoization and coordination, but transfer to a subprocess rather than remote.
    "off",  # equivalent to None - disables use of mops.
]
ShimOrBuilder = ty.Union[ShimBuilder, Shim]
logger = core.log.getLogger(__name__)
_local_root = lambda: f"file://{file_blob_store.MOPS_ROOT()}"  # noqa: E731


def _shim_name_to_builder(shim_name: ShimName) -> ty.Optional[ShimBuilder]:
    if shim_name == "samethread":
        return make_builder(samethread_shim)
    if shim_name == "subprocess":
        return make_builder(subprocess_shim)
    if shim_name == "off":
        return None
    logger.warning("Unrecognized shim name: %s; mops will be turned off.", shim_name)
    return None


def _to_shim_builder(shim: ty.Union[None, ShimName, ShimOrBuilder]) -> ty.Optional[ShimBuilder]:
    if shim is None:
        return None
    if isinstance(shim, str):
        return _shim_name_to_builder(shim)
    return make_builder(shim)


class _MagicConfig:
    def __init__(self):
        # these ConfigTree objects apply configuration to callables wrapped with pure.magic
        # based on the fully-qualified path to the callable, e.g. foo.bar.baz.my_func
        self.shim_bld = config_tree.ConfigTree[ty.Optional[ShimBuilder]](
            "mops.pure.shim", parse=_to_shim_builder  # type: ignore
        )
        self.blob_root = config_tree.ConfigTree[ty.Callable[[], str]](
            "mops.pure.blob_root", parse=uris.to_lazy_uri
        )
        self.pipeline_id = config_tree.ConfigTree[str]("mops.pure.pipeline_id")
        self.blob_root[""] = _local_root  # default Blob Store
        self.shim_bld[""] = make_builder(samethread_shim)  # default Shim
        self.pipeline_id[""] = "magic"  # default pipeline_id

    def __repr__(self) -> str:
        return f"MagicConfig(shim_bld={self.shim_bld}, blob_root={self.blob_root}, pipeline_id={self.pipeline_id})"


_MAGIC_CONFIG: ty.Final = _MagicConfig()
P = ParamSpec("P")
R = ty.TypeVar("R")


def _get_config() -> _MagicConfig:  # for testing
    return _MAGIC_CONFIG


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
        config: ty.Optional[_MagicConfig] = None,
    ):
        functools.update_wrapper(self, func)
        self._func_config_path = full_name_and_callable(func)[0].replace("--", ".")

        self.config = config or _get_config()
        if p_id := pipeline_id_mask.extract_from_docstr(func, require=False):
            # this allows the docstring pipeline id to become 'the most specific' config.
            self.config.pipeline_id.setv(p_id, self._func_config_path)
        self._shim = core.stack_context.StackContext[ty.Union[None, ShimName, ShimOrBuilder]](
            str(func) + "_SHIM", None  # none means nothing has been set stack-local
        )
        self.runner = MemoizingPicklingRunner(self._shimbuilder, self._get_blob_root)
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
            return _to_shim_builder(stack_local_shim)
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
        with pipeline_id_mask.pipeline_id_mask(self._pipeline_id):
            return self._func(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            f"Magic('{self._func_config_path}', shim={self._shim_builder_or_off},"
            f" blob_root='{self._get_blob_root()}', pipeline_id='{self._pipeline_id}')"
        )


def _magic(
    config: _MagicConfig,
    shim_or_builder: ty.Union[ShimName, ShimOrBuilder, None],
    blob_root: uris.UriResolvable,
    pipeline_id: str,
) -> ty.Callable[[ty.Callable[P, R]], Magic[P, R]]:
    def deco(func: ty.Callable[P, R]) -> Magic[P, R]:
        fully_qualified_name = full_name_and_callable(func)[0].replace("--", ".")
        if shim_or_builder is not None:
            config.shim_bld[fully_qualified_name] = _to_shim_builder(shim_or_builder)
        if blob_root:  # could be empty string
            config.blob_root[fully_qualified_name] = uris.to_lazy_uri(blob_root)
        if pipeline_id:  # could be empty string
            config.pipeline_id[fully_qualified_name] = pipeline_id
        return Magic(func, config)

    return deco


class _MagicApi:
    """The public API for this module.

    Each of these methods makes a global change to your application, so they're designed
    to be used at import time or in other situations where no functions have been called.

    If you want to apply a shim, blob_root, or pipeline_id to a single function, prefer
    the @pure.magic(shim, blob_root=your_blob_root, pipeline_id='lazing/sunday') decorator
    approach rather than configuring them after the fact, to keep the definition as close
    as possible to the site of use.
    """

    @staticmethod
    def __call__(
        shim_or_builder: ty.Union[ShimName, ShimOrBuilder, None] = None,
        *,
        blob_root: uris.UriResolvable = "",
        pipeline_id: str = "",
    ) -> ty.Callable[[ty.Callable[P, R]], Magic[P, R]]:
        return _magic(_get_config(), shim_or_builder, blob_root, pipeline_id)

    @staticmethod
    def blob_root(
        blob_root_uri: uris.UriResolvable, pathable: config_tree.Pathable = None, *, mask: bool = False
    ) -> None:
        """Sets the root URI for the blob store and control files for a specific module or function."""
        _get_config().blob_root.setv(uris.to_lazy_uri(blob_root_uri), pathable, mask=mask)

    @staticmethod
    def shim(
        shim: ty.Union[ShimName, ShimOrBuilder],
        pathable: config_tree.Pathable = None,
        *,
        mask: bool = False,
    ) -> None:
        """Use the provided shim for everything matching the pathable,
        unless there's a more specific path that matches.

        e.g.:
        - magic.shim('samethread') would turn off mops for everything within
          or below the current module.
        - magic.shim('subprocess', 'foo.bar.baz') would use the subprocess shim for
          everything within or below the foo.bar.baz module.
        - magic.shim(my_shim_builder, my_func) would use my_shim_builder for just my_func.

        To instead _mask_ everything at this level and below regardless of more specific
        config, pass mask=True.
        """
        _get_config().shim_bld.setv(_to_shim_builder(shim), pathable, mask=mask)

    @staticmethod
    def off(pathable: config_tree.Pathable = None, *, mask: bool = False) -> None:
        """Turn off mops for everything matching the pathable.

        A shortcut for shim(None).
        """
        _MagicApi.shim("off", pathable, mask=mask)

    @staticmethod
    def pipeline_id(
        pipeline_id: str, pathable: config_tree.Pathable = None, *, mask: bool = False
    ) -> None:
        """Sets the pipeline_id for a specific module or function."""
        _get_config().pipeline_id.setv(pipeline_id, pathable, mask=mask)

    @staticmethod
    def load_config_file(magic_config: ty.Optional[Path] = None) -> None:
        """Call this to load pure.magic config from the nearest .mops.toml file upward,
        or the path you provide.

        Should be called only once, in the `__main__` block of your program,
        and after all imports are resolved.
        """
        all_config = config.load(magic_config or config.first_found_config_file(), name="pure.magic")
        m_config = _get_config()
        m_config.shim_bld.load_config(all_config)
        m_config.blob_root.load_config(all_config)
        m_config.pipeline_id.load_config(all_config)

    @staticmethod
    def local_root() -> str:
        return _local_root()


magic: ty.Final = _MagicApi()
# we only instantiate this so we can have a call to magic() that is not __init__.
# there is no state whatsoever in this object.
