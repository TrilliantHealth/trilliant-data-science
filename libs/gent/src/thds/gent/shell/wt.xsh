# Xonsh integration for wt (gent - Git Ent)
# Git worktree management utilities
#
# Usage: source this file in your .xonshrc:
#   source ~/.gent/wt.xsh
#
# Or source it directly from a clone:
#   source /path/to/libs/gent/src/thds/gent/shell/wt.xsh

import os
import subprocess
import sys
from pathlib import Path

# Track if version check has run this session
_wt_version_checked = False


def _wt_check_version_once() -> None:
    """Check if shell integration is outdated (version mismatch).

    Only runs once per shell session to avoid repeated checks.
    """
    global _wt_version_checked
    if _wt_version_checked:
        return
    _wt_version_checked = True

    # Get installed version from file (missing = needs upgrade)
    installed_version = ""
    version_file = Path.home() / ".gent" / "version"
    if version_file.exists():
        try:
            installed_version = version_file.read_text().strip()
        except Exception:
            pass

    # Get package version from Python
    package_version = ""
    try:
        import thds.gent
        package_version = getattr(thds.gent, 'SHELL_INTEGRATION_VERSION', '')
    except Exception:
        pass

    # Warn if versions don't match (including missing installed version)
    if package_version and installed_version != package_version:
        print("", file=sys.stderr)
        print(f"⚠️  wt shell integration is outdated (v{installed_version or 'missing'} → v{package_version})", file=sys.stderr)
        print("   Run: wt setup-shell", file=sys.stderr)
        print("", file=sys.stderr)


def _wt_check_install_error(exit_code: int, output: str) -> bool:
    """Check for installation issues and print helpful error if detected.

    Returns True if there was a module error (caller should abort).
    """
    if exit_code != 0 and "ModuleNotFoundError" in output and "thds" in output:
        print("⚠️  wt installation issue detected", file=sys.stderr)
        print("The editable install may be pointing to a moved or deleted location.", file=sys.stderr)
        print("", file=sys.stderr)
        print("To fix, reinstall from your main worktree:", file=sys.stderr)
        print("  wt setup-shell", file=sys.stderr)
        return True

    return False


def _wt(args):
    """Main wt command - unified interface for all worktree operations.

    Navigation commands (cd, co, root) capture the target path from output
    and change directory. Other commands stream output directly.
    """
    # Check version on first use
    _wt_check_version_once()

    if not args:
        wt --help
        return 1

    cmd = args[0]
    rest = args[1:]

    # Set marker so Python knows it's running through the shell wrapper
    os.environ['_WT_SHELL_WRAPPER'] = '1'

    if cmd in ('root', 'cd', 'co'):
        # Navigation commands - capture path from last line while streaming output
        try:
            result = subprocess.run(
                ['wt', cmd, *rest],
                capture_output=True,
                text=True,
            )

            # Print any output (errors go to stderr, info to stdout)
            if result.stderr:
                print(result.stderr, end='', file=sys.stderr)

            # The last line of stdout is the target directory
            stdout_lines = result.stdout.strip().split('\n') if result.stdout.strip() else []

            # Print all but the last line (informational output)
            if len(stdout_lines) > 1:
                for line in stdout_lines[:-1]:
                    print(line)

            if _wt_check_install_error(result.returncode, result.stdout + result.stderr):
                return 1

            if result.returncode == 0 and stdout_lines:
                target = stdout_lines[-1]
                if os.path.isdir(target):
                    os.chdir(target)
                    return 0
                else:
                    return result.returncode

            return result.returncode
        except FileNotFoundError:
            print("wt command not found. Is gent installed?", file=sys.stderr)
            return 1
    else:
        # All other commands - run directly, streaming output
        @(['wt', cmd, *rest])
        return __xonsh__.lastcmd.rtn


aliases['wt'] = _wt


# Tab completion for wt command
def _wt_completer(prefix, line, begidx, endidx, ctx):
    """Xonsh completer for wt command."""
    # Parse the command line
    parts = line[:endidx].split()

    # Only complete for 'wt' command
    if not parts or parts[0] != 'wt':
        return None

    # Determine what we're completing
    # parts[0] = 'wt'
    # parts[1] = subcommand (if present)
    # parts[2+] = arguments

    def _get_completions(flag):
        """Run wt-completion and return results."""
        try:
            result = subprocess.run(
                ['wt-completion', flag],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return set(result.stdout.strip().split('\n')) if result.stdout.strip() else set()

        except FileNotFoundError:
            pass
        return set()

    # If typing 'wt ' or 'wt <partial>', complete subcommands
    if len(parts) == 1 or (len(parts) == 2 and not line.endswith(' ')):
        subcommands = _get_completions('--subcommands')
        if len(parts) == 2:
            subcommands = {s for s in subcommands if s.startswith(prefix)}
        return subcommands, len(prefix)

    # We have at least a subcommand
    subcommand = parts[1]

    # Count how many args we're on (excluding wt and subcommand)
    # If line ends with space, we're starting a new arg
    if line.endswith(' '):
        arg_position = len(parts) - 1  # -1 for 'wt', so this is 1-indexed position after subcommand
    else:
        arg_position = len(parts) - 2  # completing current arg

    if subcommand in ('cd', 'init', 'start', 'path'):
        # Complete with worktree names
        if arg_position <= 1:
            worktrees = _get_completions('--worktrees')
            worktrees = {w for w in worktrees if w.startswith(prefix)}
            return worktrees, len(prefix)

    elif subcommand == 'rm':
        # Handle --force flag
        if arg_position == 1:
            if prefix.startswith('-'):
                return {'--force', '-f'}, len(prefix)

            worktrees = _get_completions('--worktrees')
            worktrees = {w for w in worktrees if w.startswith(prefix)}
            return worktrees, len(prefix)

        elif arg_position == 2 and parts[2] in ('--force', '-f'):
            worktrees = _get_completions('--worktrees')
            worktrees = {w for w in worktrees if w.startswith(prefix)}
            return worktrees, len(prefix)

    elif subcommand in ('co', 'add'):
        # First arg: branches and worktrees, second arg: base branch
        if arg_position == 1:
            branches = _get_completions('--branches')
            worktrees = _get_completions('--worktrees')
            completions = branches | worktrees
            completions = {c for c in completions if c.startswith(prefix)}
            return completions, len(prefix)

        elif arg_position == 2:
            branches = _get_completions('--branches')
            branches = {b for b in branches if b.startswith(prefix)}
            return branches, len(prefix)

    # No completion for list, root, clone, or unknown commands
    return None


# Register the completer with high priority (runs before other completers)
__xonsh__.completers['wt'] = _wt_completer
# Move it to the front so it runs early
__xonsh__.completers.move_to_end('wt', last=False)
