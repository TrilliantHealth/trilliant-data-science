"""
Rich-based output helpers for colored terminal output and hyperlinks.
Provides consistent styling for info, success, warning, and error messages.
"""

from __future__ import annotations

import sys
from typing import NoReturn

from rich.console import Console

# Create console instances for stderr (errors/warnings/info) and stdout (success/normal output)
_console_err = Console(stderr=True)
_console_out = Console()


def info(message: str) -> None:
    """
    Print an informational message in cyan.

    Args:
        message: The message to display
    """
    _console_err.print(f"[cyan]{message}[/cyan]")


def success(message: str) -> None:
    """
    Print a success message in green.

    Args:
        message: The message to display
    """
    _console_out.print(f"[green]{message}[/green]")


def warning(message: str) -> None:
    """
    Print a warning message in yellow.

    Args:
        message: The message to display
    """
    _console_err.print(f"[yellow]Warning:[/yellow] {message}")


def error(message: str, code: int = 1) -> NoReturn:
    """
    Print an error message in red and exit.

    Args:
        message: The error message to display
        code: Exit code (default: 1)
    """
    _console_err.print(f"[red]Error:[/red] {message}")
    sys.exit(code)


def error_multiline(*lines: str, code: int = 1) -> NoReturn:
    """
    Print multi-line error message in red and exit.

    Useful for errors that need multiple lines of explanation or suggestions.

    Args:
        *lines: Lines of the error message
        code: Exit code (default: 1)

    Example:
        error_multiline(
            "Branch is not fully merged.",
            "If you are sure you want to delete it, run:",
            "  wt rm branch-name -f"
        )
    """
    _console_err.print("[red]Error:[/red]", lines[0] if lines else "")
    for line in lines[1:]:
        _console_err.print(line)
    sys.exit(code)


def link(url: str, text: str | None = None) -> str:
    """
    Create a clickable hyperlink for terminal output.

    Args:
        url: The URL to link to
        text: Optional link text (defaults to url)

    Returns:
        Rich markup string with hyperlink

    Example:
        >>> print(link("https://example.com", "Example Site"))
        # Displays as clickable "Example Site" link
    """
    display_text = text or url
    return f"[link={url}]{display_text}[/link]"


def print_output(message: str) -> None:
    """
    Print regular output to stdout (for command results).

    Args:
        message: The message to display
    """
    _console_out.print(message, highlight=False)
