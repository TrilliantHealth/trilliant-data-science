"""This is an attempt at a be-everything-to-everybody configuration 'system'.

Highlights:

- Configuration is always accessible and configurable via normal Python code.
- Configuration is type-safe.
- All active configuration is 'registered' and therefore discoverable.
- Config can be temporarily overridden for the current thread.
- Config can be set via a known environment variable.
- Config can be set by combining one or more configuration objects - these may be loaded from files,
  but this system remains agnostic as to the format of those files or how and when they are actually loaded.

"""
import typing as ty
from os import getenv

from .stack_context import StackContext

_NOT_CONFIGURED = object()


class UnconfiguredError(ValueError):
    pass


class ConfigNameCollisionError(KeyError):
    pass


def _sanitize_env(env_var_name: str) -> str:
    return env_var_name.replace("-", "_").replace(".", "_")


def _getenv(env_var_name: str) -> ty.Optional[str]:
    """We want to support a variety of naming conventions for env
    vars, without requiring people to actually name their config using
    all caps and underscores only.

    Many modern shells support more complex env var names.
    """
    return (
        getenv(env_var_name)
        or getenv(_sanitize_env(env_var_name))
        or getenv(_sanitize_env(env_var_name).upper())
    )


T = ty.TypeVar("T")


class ConfigItem(ty.Generic[T]):
    """Should only ever be constructed at a module level."""

    def __init__(
        self,
        name: str,
        default: T = ty.cast(T, _NOT_CONFIGURED),
        *,
        parse: ty.Callable[[ty.Any], T] = lambda x: x,
        allow_env_var: bool = True,
    ):
        if name in _REGISTRY:
            raise ConfigNameCollisionError(f"Config item {name} has already been registered!")
        _REGISTRY[name] = self
        self.name = name
        self.parse = parse
        if allow_env_var and _getenv(name):
            # env var is only applicable at initial creation.  if you
            # want to set this value globally after application start,
            # use set_global.
            self.global_value = parse(_getenv(name))
        else:
            self.global_value = default  # we trust your default.
        self._stack_context: StackContext[T] = StackContext(
            "config " + name, ty.cast(T, _NOT_CONFIGURED)
        )

    def set_global(self, value: T):
        """Global to the current process.

        Will not automatically get transferred to spawned processes.
        """

        self.global_value = self.parse(value)

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


class ConfigItemP(ty.Protocol[T]):
    def __call__(
        self,
        name: str,
        default: T = ty.cast(T, _NOT_CONFIGURED),
        parse: ty.Callable[[ty.Any], T] = lambda x: x,
        allow_env_var: bool = True,
    ) -> ConfigItem[T]:
        ...


def in_module(module_name: str) -> ConfigItemP:
    """In the vast majority of cases, `module(__name__)(...)` should
    be the way you name your configuration items.  It will enhance
    discoverability and clarity, and will avoid configuration name
    collisions.
    """

    def _module(name: str, *args, **kwargs) -> ConfigItem:
        return ConfigItem(f"{module_name}.{name}", *args, **kwargs)

    return ty.cast(ConfigItemP, _module)


_REGISTRY: ty.Dict[str, ConfigItem] = dict()


def config_by_name(name: str) -> ConfigItem:
    """This is a dynamic interface - in general, prefer accessing the ConfigItem object directly."""
    return _REGISTRY[name]


def set_global_defaults(config: ty.Dict[str, ty.Any]):
    """Any config-file parser can create a dictionary of only the
    items it managed to read, and then all of those can be set at once
    via this function.
    """
    for name, value in config.items():
        _REGISTRY[name].set_global(value)


def show_all_config() -> ty.Dict[str, ty.Any]:
    return {k: v() for k, v in _REGISTRY.items()}


def show_config_cli():
    import argparse
    import importlib
    from pprint import pprint

    parser = argparse.ArgumentParser()
    parser.add_argument("for_module", type=str)
    args = parser.parse_args()

    importlib.import_module(args.for_module)
    pprint(show_all_config())
