"""
Main entry point for wt command.
Dispatches to individual worktree subcommands.
"""

from __future__ import annotations

import sys
from typing import TypedDict


class CommandInfo(TypedDict):
    """Type definition for command registry entries."""

    module: str
    description: str
    usage: str


# Command registry with descriptions
COMMANDS: dict[str, CommandInfo] = {
    "clone": {
        "module": "thds.gent.commands.clone",
        "description": "Clone a repository with bare + worktree structure",
        "usage": "wt clone <url> [--directory DIR]",
    },
    "root": {
        "module": "thds.gent.commands.root",
        "description": "Find and print the worktree root directory",
        "usage": "wt root",
    },
    "cd": {
        "module": "thds.gent.commands.cd",
        "description": "Resolve path to a worktree by branch name",
        "usage": "wt cd <branch-name>",
    },
    "path": {
        "module": "thds.gent.commands.path",
        "description": "Print fully qualified path of a worktree",
        "usage": "wt path [branch-name]",
    },
    "list": {
        "module": "thds.gent.commands.list",
        "description": "List all worktrees",
        "usage": "wt list",
    },
    "rm": {
        "module": "thds.gent.commands.rm",
        "description": "Remove a worktree",
        "usage": "wt rm [--force] [branch-name]",
    },
    "init": {
        "module": "thds.gent.commands.init",
        "description": "Initialize worktree (install deps, etc.)",
        "usage": "wt init [branch-name]",
    },
    "start": {
        "module": "thds.gent.commands.start",
        "description": "Run start hook (e.g. open editor)",
        "usage": "wt start [branch-name]",
    },
    "co": {
        "module": "thds.gent.commands.co",
        "description": "Checkout existing branch or create new worktree",
        "usage": "wt co <branch-name> [base-branch]",
    },
    "setup-shell": {
        "module": "thds.gent.commands.setup_shell",
        "description": "Set up shell integration (~/.gent/)",
        "usage": "wt setup-shell",
    },
}


def show_help() -> None:
    """Display help message with all available commands."""
    print("wt - Git worktree management tool (Git Ent)")
    print()
    print("Tree shepherds for your working trees")
    print()
    print("Usage: wt <command> [options]")
    print()
    print("Commands:")

    # Find longest command name for alignment
    max_len = max(len(cmd) for cmd in COMMANDS.keys())

    for cmd, info in COMMANDS.items():
        padding = " " * (max_len - len(cmd) + 2)
        print(f"  {cmd}{padding}{info['description']}")

    print()
    print("Examples:")
    print("  wt clone git@github.com:user/repo.git    # Clone with worktree structure")
    print("  wt list                                   # List all worktrees")
    print("  wt co feature/new                         # Create worktree from current branch")
    print("  wt co feature/new main                    # Create worktree from main branch")
    print("  wt cd main                                # Navigate to main worktree")
    print("  wt init                                   # Initialize current worktree")
    print("  wt rm feature/old                         # Remove a worktree")
    print()
    print("For help on a specific command:")
    print("  wt <command> --help")


def main() -> None:
    """Main entry point."""
    # Check if any arguments provided
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "help"):
        show_help()
        sys.exit(0)

    command = sys.argv[1]

    # Check if command exists
    if command not in COMMANDS:
        print(f"Error: Unknown command: {command}", file=sys.stderr)
        print("Run 'wt --help' for usage information", file=sys.stderr)
        sys.exit(1)

    # Get command info
    cmd_info = COMMANDS[command]
    module_name = cmd_info["module"]

    # Import and execute the command module
    try:
        import importlib

        import argh

        module = importlib.import_module(module_name)

        # Remove script name and command from argv, so command sees only its args
        sys.argv = [f"wt {command}"] + sys.argv[2:]

        # Execute the command's main function via argh dispatcher
        # This handles both simple functions and argh-decorated functions
        argh.dispatch_command(module.main)
    except ImportError as e:
        print(f"Error: Failed to load command module: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
