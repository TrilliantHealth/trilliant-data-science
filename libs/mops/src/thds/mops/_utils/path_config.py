import inspect
import types
import typing as ty
from dataclasses import dataclass, field

from thds.core import log

from .names import full_name_and_callable


def _get_first_external_module(ignore_package: str = "thds.mops") -> str:
    frame = inspect.currentframe()
    if not frame:
        return ""

    try:
        while frame := frame.f_back:  # type: ignore
            module_name = frame.f_globals["__name__"]
            if not module_name.startswith(ignore_package):
                return module_name
        return ""  # fallback if no external caller found
    finally:
        del frame  # avoid reference cycles


Pathable = ty.Union[str, types.ModuleType, ty.Callable, None]
_NONE = object()
V = ty.TypeVar("V")
logger = log.getLogger(__name__)


@dataclass  # for __repr__
class PathConfig(ty.Generic[V]):
    """This is a cute little utility class for applying configuration hierarchically,
    following hierarchical paths. Generally, the 'closest' config to any given path will
    be retrieved.

    Imagine you have some modules:
    - foo.bar.baz.materialize
    - foo.bar.quux.materialize
    - foo.george.materialize
    - foo.steve.materialize

    in each of which you have several materialization functions using mops.

    Some API might construct one of these objects to afford you a way to 'set' the config
    at each level of your hierarchy.

    Inside foo.bar.__init__.py, you could call

    - `the_api.setv(a_config_object)`

    and this would set the config for anything where the module path to it included foo.bar.

    But if foo.bar.baz.materialize wanted to set config for everything inside itself,
    at the top of that module you'd call

    - `the_api.setv(diff_config_object)`

    and this would set the config for that module only.

    If you need to _override_ the config for an entire subtree, we call this masking.
    You can call the_api.setv(value, mask=True) to mask the subtree.

    - `the_api.setv(value, 'foo', mask=True)`

    will mask everything under foo, including bar, george, and steve.

    This isn't truly limited to modules, either - you can pass any module _or_ callable
    in to setv as the object from which you want us to derive a dot-separated path.
    Or you can pass in an arbitrary dot-separated string and we'll use it verbatim.
    """

    debug_name: str = ""
    configs: ty.Dict[str, V] = field(default_factory=dict)
    masks: ty.Dict[str, V] = field(default_factory=dict)

    def getv(self, path: str, default: V = ty.cast(V, _NONE)) -> V:
        parts = path.split(".")
        for i in range(0, len(parts) + 1):
            prefix = ".".join(parts[:i])
            # we do an 'in' check b/c the value might not be truthy, or even non-None
            if prefix in self.masks:
                return self.masks[prefix]

        # If not masked, fall back to normal hierarchical lookup
        return self._get_most_specific_v(path, parts, default)

    def _get_most_specific_v(
        self, path: str, parts: ty.Sequence[str], default: V = ty.cast(V, _NONE)
    ) -> V:
        for i in range(len(parts), -1, -1):
            prefix = ".".join(parts[:i])
            # we do an 'in' check b/c the value might not be truthy, or even non-None
            if prefix in self.configs:
                return self.configs[prefix]
        assert prefix == ""  # empty string means we checked the base config

        if default is not _NONE:
            return default

        name = " " if not self.debug_name else f" for {self.debug_name} "
        raise RuntimeError(f"No configuration{name}matches {path} and no global config was set")

    def setv(self, value: V, pathable: Pathable = None, *, mask: bool = False) -> None:
        """Set the value for the given Pathable, or the current module if no Pathable is given.
        By default, greater overlap in paths will supersede less overlap.

        mask=True will override any 'more specific' config below it in the hierarchy.
        """
        if isinstance(pathable, str):
            config_path = pathable
        elif pathable is None:
            config_path = _get_first_external_module()
            if not config_path:
                raise ValueError(f"Found no module outside mops within {pathable}")
        elif isinstance(pathable, types.ModuleType):
            config_path = pathable.__name__
        else:  # callable
            config_path = full_name_and_callable(pathable)[0].replace("--", ".")

        if mask:
            logger.debug(f"Masking all config under {config_path} with {value}")
            self.masks[config_path] = value
        else:
            logger.debug(f"Setting {value} for {config_path} from {pathable}")
            self.configs[config_path] = value

    def __setitem__(self, key: str, value: V) -> None:
        self.setv(value, pathable=key)
