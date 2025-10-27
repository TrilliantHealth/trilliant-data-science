"""Magic is an attempt at a new interface for mops designed to make it even less sticky
and easier to get things done with.

It's designed to combine the most common workflows into a single wrapper function
requiring an absolute minimum of boilerplate/config.

Unlike the more open-ended interface of use_runner plus BYO Runner, this one assumes
MemoizingPicklingRunner, and the most likely non-default config will be a runtime Shim or
ShimBuilder.  If you don't supply one, it will default to the same-thread shim.
"""

import typing as ty
from pathlib import Path

from thds import core
from thds.mops import config
from thds.mops._utils import config_tree

from ..core import uris
from ..runner.types import ShimBuilder
from . import sauce
from .sauce import P, R
from .shims import ShimName, ShimOrBuilder, to_shim_builder

_MAGIC_CONFIG: ty.Final = sauce.new_config()
F = ty.TypeVar("F", bound=ty.Callable)


def _get_config() -> sauce._MagicConfig:  # for testing
    return _MAGIC_CONFIG


class _MagicApi:
    """The public API for pure.magic.

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
        calls: ty.Collection[ty.Callable] = tuple(),
    ) -> ty.Callable[[ty.Callable[P, R]], sauce.Magic[P, R]]:
        """This is the main pure.magic() decorator.  It is designed to be applied directly
        at the site of function definition, i.e. on the `def`.  We dynamically capture the
        fully qualified name of the function being decorated and use that to look up the
        appropriate 'magic' configuration at the time of each call to the function. Any
        configuration passed here will be entered into the global magic config registry as
        the 'base case' for this function.

        DO NOT use this decorator multiple times on the same function, as this will overwrite
        config globally in a way that is very hard to understand.
        """
        return sauce.make_magic(_get_config(), shim_or_builder, blob_root, pipeline_id, calls)

    @staticmethod
    def deco(
        shim_or_builder: ty.Union[ShimName, ShimOrBuilder, None] = None,
        *,
        blob_root: uris.UriResolvable = "",
        pipeline_id: str = "",
        config_path: str = "",
    ) -> ty.Callable[[F], F]:  # cleaner type for certain use cases
        """This alternative API is designed for more dynamic use cases - rather than
        decorating a function def directly, you can use this to create a more generic
        decorator that can be applied within other code (not at module-level).

        However, you must never apply pure.magic.deco to the same function from multiple
        places, as this means that you have multiple different uses sharing the same
        configuration path, which will lead to subtle bugs.

        We attempt to detect this and raise an error if it happens. If it does, you should
        provide an explicit unique config_path for each usage.

        NOTE: In many cases, you may be better off using pure.magic.wand instead, which
        will allow you to prevent any 'outside' configuration from unintentionally
        affecting your function, because the explicitly-provided configuration is used but
        not entered into the global 'magic' config registry.
        """
        return ty.cast(
            ty.Callable[[F], F],
            sauce.make_magic(
                _get_config(),
                shim_or_builder,
                blob_root=blob_root,
                pipeline_id=pipeline_id,
                calls=tuple(),
                config_path=config_path,
            ),
        )

    @staticmethod
    def wand(
        shim_or_builder: ty.Union[ShimName, ShimOrBuilder, None] = None,
        *,
        blob_root: uris.UriResolvable = "",
        pipeline_id: str = "",
        calls: ty.Collection[ty.Callable] = tuple(),
    ) -> ty.Callable[[F], F]:
        """Meant for truly dynamic (i.e. runtime-controlled) use cases. This picks up
        _current_ magic configuration at the time of wrapping the function, but does not allow
        further magic configuration at the time of function call, and does not enter any
        of the supplied configuration into the global 'magic' config registry.

        Suitable for cases where you want to fall back to existing module and config-file
        configuration at the time of wrapping the function for anything that you don't supply explicitly.
        """
        return sauce.wand(
            _get_config(), shim_or_builder, blob_root=blob_root, pipeline_id=pipeline_id, calls=calls
        )

    @staticmethod
    def blob_root(
        blob_root_uri: uris.UriResolvable, pathable: config_tree.Pathable = None, *, mask: bool = False
    ) -> core.config.ConfigItem[ty.Callable[[], str]]:
        """Sets the root URI for the blob store and control files for a specific module or function."""
        return _get_config().blob_root.setv(uris.to_lazy_uri(blob_root_uri), pathable, mask=mask)

    @staticmethod
    def shim(
        shim: ty.Union[ShimName, ShimOrBuilder],
        pathable: config_tree.Pathable = None,
        *,
        mask: bool = False,
    ) -> core.config.ConfigItem[ty.Optional[ShimBuilder]]:
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
        return _get_config().shim_bld.setv(to_shim_builder(shim), pathable, mask=mask)

    @staticmethod
    def off(pathable: config_tree.Pathable = None, *, mask: bool = False) -> None:
        """Turn off mops for everything matching the pathable.

        A shortcut for shim(None).
        """
        _MagicApi.shim("off", pathable, mask=mask)

    @staticmethod
    def pipeline_id(
        pipeline_id: str, pathable: config_tree.Pathable = None, *, mask: bool = False
    ) -> core.config.ConfigItem[str]:
        """Sets the pipeline_id for a specific module or function."""
        return _get_config().pipeline_id.setv(pipeline_id, pathable, mask=mask)

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
    def config_path(func: ty.Callable) -> str:
        return sauce.make_magic_config_path(func)


magic: ty.Final = _MagicApi()
# we only instantiate this so we can have a call to magic() that is not __init__.
# there is no state whatsoever in this object.
