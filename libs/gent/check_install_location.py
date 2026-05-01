#!/usr/bin/env python3
"""Check if installing from the recommended location (main worktree)."""

import sys
from pathlib import Path


def find_worktree_root(start_path: Path) -> Path | None:
    """Find the worktree root by looking for a .bare directory."""
    current = start_path.resolve()

    while current != current.parent:
        bare_dir = current / ".bare"
        if bare_dir.exists() and bare_dir.is_dir():
            return current
        current = current.parent

    # Check root directory
    bare_dir = current / ".bare"
    if bare_dir.exists() and bare_dir.is_dir():
        return current

    return None


def main() -> None:
    """Check if installing from main worktree, output warning if not."""
    script_dir = Path(__file__).parent.resolve()
    root = find_worktree_root(script_dir)

    if root is None:
        # Not in a worktree structure, exit silently
        sys.exit(0)

    # We're in a worktree structure
    # Determine which worktree we're in by looking at the relative path
    try:
        relative_to_root = script_dir.relative_to(root)
    except ValueError:
        # Not under root (shouldn't happen)
        sys.exit(0)

    # Check if we're in main worktree
    # Path should be: main/libs/gent
    path_parts = relative_to_root.parts
    if len(path_parts) > 0 and path_parts[0] == "main":
        # Installing from main - good!
        sys.exit(0)

    # Not in main worktree - warn
    main_gent = root / "main" / "libs" / "gent"
    print(f"WARNING|{main_gent}")
    sys.exit(1)


if __name__ == "__main__":
    main()
