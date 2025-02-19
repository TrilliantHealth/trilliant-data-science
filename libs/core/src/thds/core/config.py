"""This is an attempt at a be-everything-to-everybody configuration 'system'.

Highlights:

- Configuration is always accessible and configurable via normal Python code.
- Configuration is type-safe.
- All active configuration is 'registered' and therefore discoverable.
- Config can be temporarily overridden for the current thread.
- Config can be set via a known environment variable.
- Config can be set by combining one or more configuration objects - these may be loaded from files,
  but this system remains agnostic as to the format of those files or how and when they are actually loaded.

Please see thds/core/CONFIG.md for more details.
"""

import typing as ty
from logging import getLogger
from os import getenv

from .stack_context import StackContext

_NOT_CONFIGURED = object()


class UnconfiguredError(ValueError):
    pass


class ConfigNameCollisionError(KeyError):
    pass


def _sanitize_env(env_var_name: str) -> str:
    return env_var_name.replace("-", "_").replace(".", "_")


def _getenv(name: str, secret: bool) -> ty.Optional[str]:
    """We want to support a variety of naming conventions for env
    vars, without requiring people to actually name their config using
    all caps and underscores only.

    Many modern shells support more complex env var names.
    """

    def _first_nonnil(*envvars: str) -> ty.Optional[str]:
        for envvar in envvars:
            raw_value = getenv(envvar)
            if raw_value is not None:
                lvalue = "***SECRET***" if secret else raw_value
                getLogger(__name__).info(
                    f"Loaded config '{name}' with raw value '{lvalue}' from environment variable '{envvar}'"
                )
                return raw_value
        return None

    return _first_nonnil(name, _sanitize_env(name), _sanitize_env(name).upper())


def _module_name():
    import inspect

    # this takes ~200 nanoseconds to run. that's fast enough that
    # doing the extra lookup is worth it for being more user-friendly.
    frame = inspect.currentframe()
    frame = frame.f_back.f_back.f_back  # type: ignore
    return frame.f_globals["__name__"]  # type: ignore


def _fullname(name: str) -> str:
    """In the vast majority of cases, the best way to use this library
    is to name your configuration items to correspond with your
    fully-qualified module name as a prefix.

    It will enhance discoverability and clarity,
    and will help avoid configuration name collisions.
    """
    return name if "." in name else f"{_module_name()}.{name}"


T = ty.TypeVar("T")


def _type_parser(default: T) -> ty.Callable[[ty.Any], T]:
    if default is None:
        return lambda x: x  # cannot learn anything about how to parse a future value from None
    try:
        default_type = type(default)
        if default == default_type(default):  # type: ignore
            # if this succeeds and doesn't raise, then the type is self-parsing.
            # in other words, type(4)(4) == 4, type('foobar')('foobar') == 'foobar'
            return default_type
    except Exception:
        pass
    return lambda x: x  # we can't infer a type parser, so we'll return the default no-op parser.


class ConfigRegistry(ty.Dict[str, "ConfigItem"]):  # noqa: B903
    def __init__(self, name: str):
        self.name = name


_DEFAULT_REGISTRY = ConfigRegistry("default")


class ConfigItem(ty.Generic[T]):
    """Should only ever be constructed at a module level."""

    def __init__(
        self,
        name: str,
        # names must be hierarchical. if name contains no `.`, we will force
        # your name to be in the calling module.
        default: T = ty.cast(T, _NOT_CONFIGURED),
        *,
        parse: ty.Optional[ty.Callable[[ty.Any], T]] = None,
        secret: bool = False,
        registry: ConfigRegistry = _DEFAULT_REGISTRY,
        name_transform: ty.Callable[[str], str] = _fullname,
    ):
        """parse should be an idempotent parser. In other words, parse(parse(x)) == parse(x)"""
        self.secret = secret
        name = name_transform(name)
        if name in registry:
            raise ConfigNameCollisionError(
                f"Config item {name} has already been registered in {registry.name}!"
            )
        registry[name] = self
        self.name = name
        self.parse = parse or _type_parser(default)
        raw_resolved_global = _getenv(name, secret=secret)
        if raw_resolved_global:
            # external global values are only resolved at initial
            # creation.  if you want to set this value globally after
            # application start, use set_global.
            self.global_value = self.parse(raw_resolved_global)
        elif default is not _NOT_CONFIGURED:
            self.global_value = self.parse(default)
        else:
            self.global_value = default
        self._stack_context: StackContext[T] = StackContext(
            "config_" + name, ty.cast(T, _NOT_CONFIGURED)
        )

    def set_global(self, value: T):
        """Global to the current process.

        Will not automatically get transferred to spawned processes.
        """

        self.global_value = value
        # we used to parse this value, but I think that was the wrong choice -
        # we should only have to parse it when it's coming from the environment,
        # which should be handled by it's initial creation,
        # or when it's being set as a global default from a config file.

    def set_local(self, value: T) -> ty.ContextManager[T]:
        """Local to the current thread.

        Will not automatically get transferred to spawned threads.
        """

        return self._stack_context.set(value)

    def __call__(self) -> T:
        local = self._stack_context()
        if local is not _NOT_CONFIGURED:
            return local
        if self.global_value is _NOT_CONFIGURED:
            raise UnconfiguredError(f"Config item '{self.name}' has not been configured!")
        return self.global_value

    def __repr__(self) -> str:
        return f"ConfigItem('{self.name}', {self()})"


def tobool(s_or_b: ty.Union[str, bool]) -> bool:
    """A reasonable implementation that we could expand in the future."""
    return s_or_b if isinstance(s_or_b, bool) else s_or_b.lower() not in ("0", "false", "no", "off", "")


def maybe(parser: ty.Callable[[ty.Any], T]) -> ty.Callable[[ty.Optional[ty.Any]], ty.Optional[T]]:
    """A helper for when you want to parse a value that might be nil."""
    return lambda x: parser(x) if x is not None else None


item = ConfigItem
# a short alias


def config_by_name(
    name: str,
    registry: ConfigRegistry = _DEFAULT_REGISTRY,
) -> ConfigItem:
    """This is a dynamic interface - in general, prefer accessing the ConfigItem object directly."""
    return registry[_fullname(name)]


def flatten_config(config: ty.Mapping[str, ty.Any]) -> ty.Dict[str, ty.Any]:
    """This is a helper function to flatten a nested configuration dictionary."""
    flat = dict()
    for key, value in config.items():
        if isinstance(value, dict):
            for subkey, subvalue in flatten_config(value).items():
                flat[f"{key}.{subkey}"] = subvalue
        else:
            flat[key] = value
    return flat


def set_global_defaults(
    config: ty.Mapping[str, ty.Any],
    registry: ConfigRegistry = _DEFAULT_REGISTRY,
):
    """Any config-file parser can create a dictionary of only the
    items it managed to read, and then all of those can be set at once
    via this function.
    """
    flat_config = flatten_config(config)
    for name, value in flat_config.items():
        try:
            config_item = registry[name]
            config_item.set_global(config_item.parse(value))
        except KeyError:
            # try directly importing a module - this is only best-effort and will not work
            # if you did not follow standard configuration naming conventions.
            import importlib

            maybe_module_name = ".".join(name.split(".")[:-1])

            try:
                importlib.import_module(maybe_module_name)
                try:
                    config_item = registry[name]
                    config_item.set_global(config_item.parse(value))
                except KeyError as kerr:
                    raise KeyError(
                        f"Config item {name} is not registered"
                        f" and no module with the name {maybe_module_name} was importable."
                        " Please double-check your configuration."
                    ) from kerr
            except ModuleNotFoundError:
                # create a new, dynamic config item that will only be accessible via
                # its name.
                ConfigItem(
                    name, value, registry=registry
                )  # return value not needed since it self-registers.
                getLogger(__name__).debug(
                    "Created dynamic config item '%s' with value '%s'", name, value
                )


def get_all_config(registry: ty.Dict[str, ConfigItem] = _DEFAULT_REGISTRY) -> ty.Dict[str, ty.Any]:
    return {k: v() if not v.secret else "***SECRET***" for k, v in registry.items()}


def show_config_cli():
    import argparse
    import importlib
    from pprint import pprint

    parser = argparse.ArgumentParser()
    parser.add_argument("via_modules", type=str, nargs="+")
    args = parser.parse_args()

    for module in args.via_modules:
        importlib.import_module(module)

    print()
    print("thds.core.config")
    print("----------------")
    print("The following keys are fully-qualified module paths by default.")
    print(
        "A given item can be capitalized and set via environment variable"
        ", e.g. export THDS_CORE_LOG_LEVEL='DEBUG'"
    )
    print(
        "An item can also be set globally or locally, after importing the"
        " ConfigItem object from the Python module where it is defined."
    )
    print()
    pprint(get_all_config())
