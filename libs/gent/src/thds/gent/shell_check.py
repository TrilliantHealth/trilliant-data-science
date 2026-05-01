"""
Shell integration checks for gent.

Provides utilities to detect when navigation commands are being run
without the shell wrapper, which means directory changes won't work.
"""

from __future__ import annotations

import os
import sys

# Environment variable set by the shell wrapper (wt.sh)
_SHELL_WRAPPER_ENV = "_WT_SHELL_WRAPPER"


def is_shell_wrapper_active() -> bool:
    """Check if the command is being run through the shell wrapper."""
    return os.environ.get(_SHELL_WRAPPER_ENV) == "1"


def warn_if_no_shell_wrapper() -> None:
    """Print a warning if shell integration is not active.

    Called by navigation commands (cd, co, root) that output paths for the
    shell wrapper to cd into. Without the wrapper, the path is printed but
    the directory doesn't change.
    """
    if is_shell_wrapper_active():
        return

    print(
        "\n\033[33m⚠️  Shell integration not active - directory was NOT changed.\033[0m\n"
        "   To fix: source ~/.gent/init.sh (and add to your shell RC file)\n",
        file=sys.stderr,
    )
