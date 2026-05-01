"""
Set up shell integration for wt.
Used by the 'wt setup-shell' command.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from thds.gent import SHELL_INTEGRATION_VERSION, output

GENT_DIR = Path.home() / ".gent"

INIT_SH_CONTENT = """\
# wt initialization wrapper
# This file is managed by wt - do not edit manually

# Always source shell integration (provides error handling even if module is broken)
if [ -f ~/.gent/wt.sh ]; then
    source ~/.gent/wt.sh
fi

# Check if Python module is importable and warn if not
if command -v wt >/dev/null 2>&1; then
    WT_BIN="$(command -v wt)"
    if [ -f "$WT_BIN" ]; then
        WT_PYTHON="$(head -n 1 "$WT_BIN" | sed 's/^#!//')"
        if [ -x "$WT_PYTHON" ]; then
            if ! "$WT_PYTHON" -c 'import thds.gent' 2>/dev/null; then
                echo "⚠️  wt installation issue detected" >&2
                echo "The thds.gent module is not importable; the install may be broken." >&2
                echo "" >&2
                echo "To fix, reinstall thds.gent and re-run setup-shell:" >&2
                echo "  pip install --upgrade thds.gent && wt setup-shell" >&2
                echo "" >&2
            fi
        fi
    fi
fi
"""

XONSH_SOURCE_BLOCK = """\
# wt - Git worktree utilities
from pathlib import Path as _GentPath
if _GentPath('~/.gent/wt.xsh').expanduser().exists():
    source ~/.gent/wt.xsh
del _GentPath
"""

POSIX_SOURCE_LINE = "[ -f ~/.gent/init.sh ] && source ~/.gent/init.sh"


def _get_shell_dir() -> Path:
    """Get the path to gent's bundled shell scripts."""
    return Path(__file__).parent.parent / "shell"


def _copy_shell_files() -> None:
    """Copy shell integration files from the installed package to ~/.gent/."""
    GENT_DIR.mkdir(parents=True, exist_ok=True)

    shell_dir = _get_shell_dir()
    for filename in ("wt.sh", "wt-completion.bash", "wt-completion.zsh", "wt.xsh"):
        src = shell_dir / filename
        if src.exists():
            shutil.copy2(src, GENT_DIR / filename)

    output.success("Copied shell integration files to ~/.gent/")


def _write_init_sh() -> None:
    """Write the ~/.gent/init.sh wrapper script."""
    (GENT_DIR / "init.sh").write_text(INIT_SH_CONTENT)
    output.success("Created ~/.gent/init.sh")


def _write_version() -> None:
    """Write the shell integration version file."""
    (GENT_DIR / "version").write_text(SHELL_INTEGRATION_VERSION)
    output.success(f"Wrote infrastructure version ({SHELL_INTEGRATION_VERSION}) to ~/.gent/version")


def _detect_shell_configs() -> tuple[Path | None, Path | None]:
    """Detect POSIX shell and xonsh config files.

    Returns:
        Tuple of (posix_config_path, xonsh_config_path), either may be None.
    """
    home = Path.home()
    posix_config = None
    xonsh_config = None

    # Prefer zshrc, fall back to bashrc
    if (home / ".zshrc").exists():
        posix_config = home / ".zshrc"
    elif (home / ".bashrc").exists():
        posix_config = home / ".bashrc"

    if (home / ".xonshrc").exists():
        xonsh_config = home / ".xonshrc"

    return posix_config, xonsh_config


def _append_source_block(config_path: Path, marker: str, block: str, label: str) -> None:
    """Append `block` to `config_path` if `marker` is not already present.

    Creates the RC file if it does not exist.
    """
    if config_path.exists():
        if marker in config_path.read_text():
            output.info(f"{label} already configured in {config_path}")
            return

    with open(config_path, "a") as f:
        f.write(block)
    output.success(f"Added {label.lower()} to {config_path}")


def _configure_posix_shell(config_path: Path) -> None:
    _append_source_block(
        config_path,
        marker=".gent/init.sh",
        block=f"\n# wt - Git worktree utilities\n{POSIX_SOURCE_LINE}\n",
        label="Shell integration",
    )


def _configure_xonsh(config_path: Path) -> None:
    _append_source_block(
        config_path,
        marker=".gent/wt.xsh",
        block=f"\n{XONSH_SOURCE_BLOCK}",
        label="Xonsh integration",
    )


def main() -> None:
    """Set up shell integration for wt (gent).

    Copies shell scripts to ~/.gent/, configures your shell RC file, and
    enables tab completion. Run this after installing gent via pip or uv.

    This command is safe to re-run; it will update shell scripts and skip
    RC file changes that are already present.

    Examples:
      wt setup-shell         # Set up shell integration
      pip install thds.gent  # Then run: wt setup-shell
    """
    output.info("Setting up shell integration for wt...")
    print()

    _copy_shell_files()
    _write_init_sh()
    _write_version()

    print()

    posix_config, xonsh_config = _detect_shell_configs()

    if posix_config is None and xonsh_config is None:
        output.warning(
            "Could not detect shell type.\n"
            f"Please add this to your shell RC file:\n  {POSIX_SOURCE_LINE}"
        )
    else:
        if posix_config is not None:
            _configure_posix_shell(posix_config)
        if xonsh_config is not None:
            _configure_xonsh(xonsh_config)

    print()
    output.success("Shell integration setup complete!")
    print()
    output.info("To use wt now:")
    print("  source your shell RC file, or open a new terminal")
    print()
    output.info("Verify installation:")
    print("  wt --help")
    print("  wt list")
