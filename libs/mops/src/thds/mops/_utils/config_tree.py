import inspect
import types
import typing as ty
from functools import partial

from thds import core

from .names import full_name_and_callable

IGNORED_PACKAGES = ["thds.mops"]
# if a library needs to build on top of mops, it can put itself into this
# list and we'll ignore it when looking for the 'true calling frame'


def _get_first_external_module(ignore_packages: ty.Collection[str] = IGNORED_PACKAGES) -> str:
    frame = inspect.currentframe()
    if not frame:
        return ""

    while frame := frame.f_back:  # type: ignore
        module_name = frame.f_globals["__name__"]
        is_ignored = False
        for ignore_package in ignore_packages:
            if module_name.startswith(ignore_package):
                is_ignored = True
                break

        if not is_ignored:
            return module_name
    return ""  # fallback if no external caller found


Pathable = ty.Union[str, types.ModuleType, ty.Callable, None]
_NONE = object()
V = ty.TypeVar("V")
logger = core.log.getLogger(__name__)


def to_dotted_path(pathable: Pathable) -> str:
    if isinstance(pathable, str):
        return pathable

    if pathable is None:
        if not (module_path := _get_first_external_module()):
            raise ValueError(f"Found no module outside mops within {pathable}")
        return module_path

    if isinstance(pathable, types.ModuleType):
        return pathable.__name__

    return full_name_and_callable(pathable)[0].replace("--", ".")


class ConfigTree(ty.Generic[V]):
    """This is a cute little utility class for applying homogeneously-typed configuration
    following hierarchical (tree-like) paths.

    Generally, the config closest to the 'leaf' path will be used, but there is also a
    'mask' option to override subtrees.

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

    def __init__(self, name: str, parse: ty.Optional[ty.Callable[[ty.Any], V]] = None):
        """If provided, parse must be an idempotent parser. In other words, parse(parse(x)) == parse(x)"""
        self.registry = core.config.ConfigRegistry(name)
        self.parse = parse or (lambda v: v)
        self._make_config = partial(
            core.config.ConfigItem[V], registry=self.registry, name_transform=lambda s: s, parse=parse
        )

    def getv(self, path: str, default: V = ty.cast(V, _NONE)) -> V:
        parts = [*path.split(".")]
        mask = "__mask"
        for i in range(0, len(parts) + 1):
            prefix = ".".join([mask, *parts[:i]])
            # we do an 'in' check b/c the value might not be truthy, or even non-None
            if prefix in self.registry:
                return self.registry[prefix]()

        # If not masked, fall back to normal hierarchical lookup
        return self._get_most_specific_v(path, parts, default)

    def _get_most_specific_v(
        self, path: str, parts: ty.Sequence[str], default: V = ty.cast(V, _NONE)
    ) -> V:
        for i in range(len(parts), -1, -1):
            prefix = ".".join(parts[:i])
            # we do an 'in' check b/c the value might not be truthy, or even non-None
            if prefix in self.registry:
                return self.registry[prefix]()
        assert prefix == ""

        if default is not _NONE:
            return default

        name = self.registry.name
        raise RuntimeError(f"No {name} configuration matches {path} and no global config was set")

    def setv(
        self, value: V, pathable: Pathable = None, *, mask: bool = False
    ) -> core.config.ConfigItem[V]:
        """Set the value for the given Pathable, or the current module if no Pathable is given.
        By default, greater overlap in paths will supersede less overlap.

        mask=True will override any 'more specific' config below it in the hierarchy.
        """
        config_path = to_dotted_path(pathable)
        if mask:
            config_path = ".".join(filter(None, ["__mask", config_path]))
            log_msg = "Masking all [%s] config under '%s' with %s"
        else:
            log_msg = "Setting [%s] '%s' to %s"
        logger.debug(log_msg, self.registry.name, config_path, value)
        if config_item := self.registry.get(config_path):
            config_item.set_global(self.parse(value))
        else:
            config_item = self._make_config(config_path, default=value)  # also registers the ConfigItem
        return config_item

    def __setitem__(self, key: str, value: V) -> None:
        self.setv(value, pathable=key)

    def load_config(self, config: ty.Mapping[str, ty.Any]) -> None:
        """Loads things with an inner key matching this name into the config.

        The config looks something like this:

        thds.modulea.foobar.funcname.(?__mask).mops.pure.magic.pipeline_id = 'force-rerun'

        The end part must correspond to the 'name' of this ConfigTree, e.g. mops.pure.magic.pipeline_id,
        so that we can identify which part of this config is relevant to us.

        We then split off that part, plus any __mask prefix that might or might not be present.
        The remaining part (from the beginning) is then the 'pathable' - the part of the tree
        that this config value applies to. The value is, of course, the value for that tree.
        """
        mask_name = f".__mask.{self.registry.name}"
        conf_name = f".{self.registry.name}"  # e.g. .mops.pure.magic.pipeline_id
        logger.debug("Loading config for %s", self.registry.name)
        for key, value in core.config.flatten_config(config).items():
            if key.endswith(conf_name):  # then this is for us.
                mask = key.endswith(mask_name)  # then this is a mask
                pathable = key[: -len(mask_name if mask else conf_name)]
                self.setv(value, pathable, mask=mask)

    def __repr__(self) -> str:
        return f"ConfigTree('{self.registry.name}', {list(self.registry.items())})"
