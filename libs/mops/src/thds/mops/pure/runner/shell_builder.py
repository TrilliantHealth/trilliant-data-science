import inspect
import typing as ty

from ..core.types import Args, F, Kwargs
from .types import Shell, ShellBuilder


def make_builder(shell: ty.Union[Shell, ShellBuilder]) -> ShellBuilder:
    """If you have a Shell and you want to make it into the simplest possible ShellBuilder."""

    if len(inspect.signature(shell).parameters) == 3:
        return ty.cast(ShellBuilder, shell)

    def static_shell_builder(_f: F, _args: Args, _kwargs: Kwargs) -> Shell:
        return ty.cast(Shell, shell)

    return ty.cast(ShellBuilder, static_shell_builder)
