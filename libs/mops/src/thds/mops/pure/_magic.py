"""Magic is an attempt at a new interface for mops designed to make it even less sticky
and easier to get things done with.

It's designed to combine the most common workflows into a single wrapper function
requiring an absolute minimum of boilerplate/config.

Unlike the more open-ended interface of use_runner plus BYO Runner, this one assumes
MemoizingPicklingRunner, and the most likely non-default config will be a runtime Shim or
ShimBuilder.  If you don't supply one, it will default to the same-thread shell.
"""

import contextlib
import functools
import typing as ty

from typing_extensions import ParamSpec

from thds import core
from thds.mops import config
from thds.mops._utils import path_config

from .core import file_blob_store, pipeline_id_mask, uris
from .core.memo.unique_name_for_function import full_name_and_callable
from .core.use_runner import use_runner
from .pickling.mprunner import MemoizingPicklingRunner
from .runner import Shell, ShellBuilder
from .runner.shell_builder import make_builder
from .runner.simple_shims import samethread_shim, subprocess_shim

ShimName = ty.Literal[
    "samethread",  # memoization and coordination, but run in the same thread as the caller.
    "subprocess",  # memoization and coordination, but transfer to a subprocess rather than remote.
    "off",  # equivalent to None - disables use of mops.
]
ShimOrBuilder = ty.Union[ShellBuilder, Shell]


def _shim_name_to_builder(shim_name: ShimName) -> ty.Optional[ShellBuilder]:
    if shim_name == "samethread":
        return make_builder(samethread_shim)
    if shim_name == "subprocess":
        return make_builder(subprocess_shim)
    if shim_name == "off":
        return None
    logger.warning("Unrecognized shim name: %s; mops will be turned off.", shim_name)
    return None


_CONF_PREFIX = "mops.pure.magic"


class _MagicConfig:
    def __init__(self):
        # these PathConfig objects apply configuration to callables wrapped with pure.magic
        # based on the fully-qualified path to the callable, e.g. foo.bar.baz.my_func
        self.shim_bld = path_config.PathConfig[ty.Optional[ShellBuilder]]("Shim Builder")
        self.blob_root = path_config.PathConfig[ty.Callable[[], str]]("Blob Root")

    def get_path(self, config_key: str, key: str) -> str:
        conf_prefix = f"{_CONF_PREFIX}.{config_key}."
        if key.startswith(conf_prefix):
            return key[len(conf_prefix) :]
        return ""

    def add_dynamic_config(self, config: ty.Mapping[str, ty.Any]) -> None:
        """Add dynamic configuration to the MagicConfig, e.g. from the mops config"""
        for key, value in config.items():
            if pathable := self.get_path("blob_store", key):
                self.blob_root[pathable] = uris.to_lazy_uri(value)
            elif pathable := self.get_path("shim", key):
                self.shim_bld[pathable] = _shim_name_to_builder(value)
            elif pathable := self.get_path("__mask.blob_root", key):
                self.blob_root.setv(uris.to_lazy_uri(value), pathable, mask=True)
            elif pathable := self.get_path("__mask.shim", key):
                self.shim_bld.setv(_shim_name_to_builder(value), pathable, mask=True)


_MAGIC_CONFIG = _MagicConfig()
_local_root = lambda: f"file://{file_blob_store.MOPS_ROOT()}"  # noqa: E731
_MAGIC_CONFIG.blob_root[""] = _local_root  # default Blob Store
_MAGIC_CONFIG.shim_bld[""] = make_builder(samethread_shim)  # default Shim
_MAGIC_CONFIG.add_dynamic_config(config.dynamic_mops_config())
logger = core.log.getLogger(__name__)
P = ParamSpec("P")
R = ty.TypeVar("R")


class Magic(ty.Generic[P, R]):
    """Magic adds mops' powers (memoization, coordination, remote execution) to a callable.

    You can completely disable mops magic for a function by opening a context with the function,
    like so:

    with my_magic_func.off():
        ...
        my_magic_func(1, 2, 3)

    If you want to change which runtime shim the function is using, that can be set globally
    to the program with pure.magic.shim(other_shim, my_magic_func), or you can choose a named
    shim with pure.magic.shim("subprocess", my_magic_func).
    """

    def __init__(
        self,
        func: ty.Callable[P, R],
        config: _MagicConfig = _MAGIC_CONFIG,
    ):
        functools.update_wrapper(self, func)
        self._func_config_path = full_name_and_callable(func)[0].replace("--", ".")

        self.config = config
        self._shim = core.stack_context.StackContext[ty.Optional[ShellBuilder]](
            str(func) + "_SHIM", None
        )
        self.runner = MemoizingPicklingRunner(self._shimbuilder, self._get_blob_root)
        self.pipeline_id_mask = pipeline_id_mask.extract_mask_from_docstr(func, require=False) or "magic"
        self._func = use_runner(self.runner, self._is_off)(func)
        self.__doc__ = f"{func.__doc__}\n\nMagic class info:\n{self.__class__.__doc__}"
        self.__wrapped__ = func

    @contextlib.contextmanager
    def shim(self, shim_or_builder: ty.Optional[ShimOrBuilder]) -> ty.Iterator[None]:
        with self._shim.set(make_builder(shim_or_builder) if shim_or_builder else None):
            yield

    @contextlib.contextmanager
    def off(self) -> ty.Iterator[None]:
        """off is an API for setting the shim to None,
        effectively turning off mops for the wrapped function.
        """
        with self.shim(None):
            yield

    @property
    def _shim_builder_cfg(self) -> ty.Optional[ShellBuilder]:
        return self._shim() or self.config.shim_bld.getv(self._func_config_path)

    def _is_off(self) -> bool:
        return self._shim_builder_cfg is None

    def _shimbuilder(self, f: ty.Callable[P, R], args: P.args, kwargs: P.kwargs) -> Shell:
        # this can be set using a stack-local context, or set globally as specifically
        # or generally as the user needs. We prefer stack local over everything else.
        sb = self._shim_builder_cfg
        assert sb is not None, "This should have been handled by use_runner(self._off)"
        return sb(f, args, kwargs)

    def _get_blob_root(self) -> str:
        return self.config.blob_root.getv(self._func_config_path)()

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """This is the wrapped function."""
        with pipeline_id_mask.pipeline_id_mask(self.pipeline_id_mask):
            return self._func(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Magic({self._func_config_path})"


_MAGIC_VAULT: ty.Dict[str, Magic] = dict()
# not sure what we're going to use this for but I have a feeling we'll want it.


def _to_shim_builder(shim: ty.Union[None, ShimName, ShimOrBuilder]) -> ty.Optional[ShellBuilder]:
    if isinstance(shim, str):
        return _shim_name_to_builder(shim)
    elif shim is not None:
        return make_builder(shim)
    return None


def _magic(
    config: _MagicConfig,
    shim_or_builder: ty.Union[ShimName, ShimOrBuilder, None],
    blob_root: uris.UriResolvable,
) -> ty.Callable[[ty.Callable[P, R]], Magic[P, R]]:
    def deco(func: ty.Callable[P, R]) -> Magic[P, R]:
        fully_qualified_name = full_name_and_callable(func)[0].replace("--", ".")
        if shim_or_builder is not None:
            config.shim_bld[fully_qualified_name] = _to_shim_builder(shim_or_builder)
        if blob_root:  # could be empty string
            config.blob_root[fully_qualified_name] = uris.to_lazy_uri(blob_root)
        magic_func = Magic(func)
        _MAGIC_VAULT[fully_qualified_name] = magic_func
        return magic_func

    return deco


class _MagicApi:
    """The public API for this module.

    Entirely static methods, but this way we can do magic() or magic.shim(), etc.
    """

    @staticmethod
    def __call__(
        shim_or_builder: ty.Union[ShimName, ShimOrBuilder, None] = None,
        *,
        blob_root: uris.UriResolvable = "",
    ) -> ty.Callable[[ty.Callable[P, R]], Magic[P, R]]:
        return _magic(_MAGIC_CONFIG, shim_or_builder, blob_root)

    @staticmethod
    def blob_root(
        blob_root_uri: uris.UriResolvable, pathable: path_config.Pathable = None, *, mask: bool = False
    ) -> None:
        """Sets the root URI for the blob store and control files for a specific module or function."""
        _MAGIC_CONFIG.blob_root.setv(uris.to_lazy_uri(blob_root_uri), pathable, mask=mask)

    @staticmethod
    def shim(
        shim: ty.Union[None, ShimName, ShimOrBuilder],
        pathable: path_config.Pathable = None,
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
        _MAGIC_CONFIG.shim_bld.setv(_to_shim_builder(shim), pathable, mask=mask)

    @staticmethod
    def off(pathable: path_config.Pathable = None, *, mask: bool = False) -> None:
        """Turn off mops for everything matching the pathable.

        A shortcut for shim(None).
        """
        _MagicApi.shim(None, pathable, mask=mask)

    @staticmethod
    def local_root() -> str:
        return _local_root()


magic: ty.Final = _MagicApi()
# we only instantiate this so we can have a call to magic() that is not __init__.
# there is no state whatsoever in this object.
