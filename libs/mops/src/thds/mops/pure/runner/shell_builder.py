import inspect
import typing as ty

from ..core.types import Args, F, Kwargs
from .types import Shell, ShellBuilder


class _static_shell_builder:
    def __init__(self, shell: Shell) -> None:
        self.shell = shell

    def __call__(self, _f: F, _args: Args, _kwargs: Kwargs) -> Shell:
        return self.shell

    def __repr__(self) -> str:
        return f"<static_shell_builder for {self.shell}>"


def make_builder(shell: ty.Union[Shell, ShellBuilder]) -> ShellBuilder:
    """If you have a Shell and you want to make it into the simplest possible ShellBuilder."""

    if len(inspect.signature(shell).parameters) == 3:
        return ty.cast(ShellBuilder, shell)

    return _static_shell_builder(ty.cast(Shell, shell))
