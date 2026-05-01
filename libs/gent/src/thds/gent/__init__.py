"""Git worktree management utilities (gent - Git Ent)."""

# Library version - pulled from pyproject.toml, tracks all changes
# TODO: Remove importlib.metadata fallback once all users have upgraded to
# SHELL_INTEGRATION_VERSION 1 (which requires reinstalling with install.sh)
try:
    from thds.core import meta

    __version__ = meta.get_version(__name__)
except ImportError:
    from importlib.metadata import version

    __version__ = version("thds.gent")

# Shell integration version - bump ONLY when shell scripts or install.sh change.
# This is separate from __version__ because shell scripts are copied to ~/.gent/
# during install, so Python code updates don't require re-running install.sh.
# When this doesn't match ~/.gent/version, users see a warning to reinstall.
SHELL_INTEGRATION_VERSION = "4"
