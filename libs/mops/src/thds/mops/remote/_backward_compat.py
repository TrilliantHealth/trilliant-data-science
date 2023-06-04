"""Shims for backward compatibility."""
import typing as ty

from thds.adls import AdlsFqn, AdlsRoot

from ..config import get_memo_storage_root
from .pickle_runner import MemoizingPickledFunctionRunner
from .types import Shell, ShellBuilder, _ShellBuilder


def AdlsPickleRunner(
    shell: ty.Union[Shell, ShellBuilder],
    adls_path: ty.Union[AdlsRoot, AdlsFqn, None] = None,
    rerun_exceptions: bool = False,
) -> MemoizingPickledFunctionRunner:
    """Backward-compatible shim for MemoizingPickledFunctionRunner."""
    return MemoizingPickledFunctionRunner(
        # this is for backward compatibility. arguably we should simplify this interface.
        (
            ty.cast(_ShellBuilder, shell.shell_builder)
            if isinstance(shell, ShellBuilder)
            else lambda _f, *_args, **_kws: shell
        ),
        str(adls_path or get_memo_storage_root()),
        rerun_exceptions=rerun_exceptions,
    )
