# Developing gent

## Architecture

gent has three components:

| Component     | Location          | Description                                   |
| ------------- | ----------------- | --------------------------------------------- |
| Python code   | `src/thds/gent/`  | Core logic, commands, utilities               |
| Shell scripts | `~/.gent/*.sh`    | Wrappers that enable `cd` in the parent shell |
| Entry points  | `~/.local/bin/wt` | CLI executables registered via pyproject.toml |

Shell scripts are **copied** to `~/.gent/` during installation. This is necessary because they must be
sourced into the user's shell to enable directory changes.

## Installation Modes

gent supports two installation modes:

| Mode         | Command                          | Python Updates      | Shell Script Updates |
| ------------ | -------------------------------- | ------------------- | -------------------- |
| Editable     | `./install.sh` (from checkout)   | Automatic (on pull) | `wt setup-shell`     |
| Non-editable | `pip install` + `wt setup-shell` | Manual (reinstall)  | `wt setup-shell`     |

**Editable mode** is recommended for developers working on gent itself. `install.sh` runs
`uv tool install --editable` and then `wt setup-shell`. Python code changes take effect immediately when
you pull, without reinstalling.

**Non-editable mode** (`pip install thds.gent`) installs a frozen copy. Both Python code and shell
scripts update together when you reinstall. Run `wt setup-shell` after installing.

## Versioning

gent has two version numbers:

| Version                     | Location       | Tracks                    | When to Bump                                 |
| --------------------------- | -------------- | ------------------------- | -------------------------------------------- |
| `__version__`               | pyproject.toml | All library changes       | Any change (auto from pyproject.toml)        |
| `SHELL_INTEGRATION_VERSION` | `__init__.py`  | Shell script changes only | Only when shell scripts or install.sh change |

**Why two versions?** For editable installs, Python code updates automatically but shell scripts (which
are copied to `~/.gent/`) do not. The `SHELL_INTEGRATION_VERSION` lets the shell wrapper detect when it's
outdated relative to the Python package and prompt users to re-run `wt setup-shell`.

For non-editable installs, both components update together on reinstall, so the version check is less
critical but still useful for detecting partial upgrade states.

**Bump `SHELL_INTEGRATION_VERSION` when changing:**

- Shell scripts (`src/thds/gent/shell/*.sh`, `*.xsh`)
- Entry points in `pyproject.toml`
- The `init.sh` template in `setup_shell.py`

**Do NOT bump for:** Pure Python changes, docs, or bug fixes that don't affect the shell interface.

## Shell Wrapper Detection

Navigation commands (`cd`, `co`, `root`) output paths for the shell wrapper to `cd` into. The wrapper
sets `_WT_SHELL_WRAPPER=1` so Python can detect when it's missing and warn users.

This catches cases where users have the `wt` entry point but haven't sourced the shell integration.

## Common Tasks

**Adding a command:**

1. Create `src/thds/gent/commands/newcmd.py` with `main()` function
1. Register in `__main__.py` COMMANDS dict
1. If it outputs a navigation path, call `warn_if_no_shell_wrapper()`

**Modifying shell scripts:**

1. Edit `src/thds/gent/shell/`
1. Bump `SHELL_INTEGRATION_VERSION` in `__init__.py`
1. Test with `wt setup-shell`
1. Document in `CHANGES.md`

## Debugging

```bash
cat ~/.gent/version                                                        # Installed shell version
python3 -c 'import thds.gent; print(thds.gent.SHELL_INTEGRATION_VERSION)'  # Package shell version
type wt                                                                    # Should be "shell function"
```
