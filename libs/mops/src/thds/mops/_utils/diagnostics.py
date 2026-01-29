"""Diagnostic information collection for debugging mops invocations.

This module provides functions to gather system and environment information
that helps debug mops invocations, especially when running remotely (e.g.,
in Kubernetes).
"""

import importlib.metadata
import platform
import traceback
import typing as ty


def _format_traceback(exc: Exception) -> ty.List[str]:
    tb_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return [line.rstrip() for line in tb_text.split("\n")]


def _get_installed_packages() -> ty.List[str]:
    """Get all installed packages with versions, sorted alphabetically."""
    packages = []
    for dist in importlib.metadata.distributions():
        name = dist.metadata.get("Name", "")
        version = dist.version
        if name:
            packages.append(f"{name}=={version}")

    return sorted(packages, key=str.lower)


def _get_environment_info() -> ty.List[str]:
    return [
        f"python_version={platform.python_version()}",
        f"python_implementation={platform.python_implementation()}",
        f"platform={platform.platform()}",
    ]


def format_exception_diagnostics(exc: Exception) -> str:
    """Format diagnostic information for an exception.

    Returns a multi-line string containing:
    - Exception type and message
    - Full stack trace
    - Python version and platform info
    - All installed packages with versions

    This information helps debug remote execution failures without needing
    to unpickle the exception or access the original environment.
    """
    lines = [
        "",
        "=== Exception ===",
        f"type={type(exc).__module__}.{type(exc).__name__}",
        # message might have newlines or other characters, but that's fine for this section
        f"message={exc}",
        "",
        "=== Stack Trace ===",
        *_format_traceback(exc),
        "",
        "=== Environment ===",
        *_get_environment_info(),
        "",
        "=== Installed Packages ===",
        *_get_installed_packages(),
    ]

    return "\n".join(lines) + "\n"


def format_environment_diagnostics() -> str:
    """Format environment diagnostic information (no exception).

    Returns a multi-line string containing:
    - Python version and platform info
    - All installed packages with versions

    This is used for successful results that took a long time, to help
    debug environment-related issues without needing access to the
    original execution environment.
    """
    lines = [
        "",
        "=== Environment ===",
        *_get_environment_info(),
        "",
        "=== Installed Packages ===",
        *_get_installed_packages(),
    ]

    return "\n".join(lines) + "\n"
