# wt (The Git Ent)

> _"We are tree-herds, we old Ents. Few enough of us are left. Sheep get like shepherds, and shepherds
> like sheep, it is said; but slowly, and neither have long in the world. It is quicker and closer with
> trees and Ents, and they walk down the ages together."_ — Treebeard, The Two Towers

Git worktree management utilities - tree shepherds for your working trees.

**Why use gent?** Git worktrees are powerful but awkward to use directly. gent makes worktrees simple by
establishing a clear directory convention (worktrees, branches, and directories share the same name) and
providing simple commands for common operations.

## About Ents

In J.R.R. Tolkien's Middle-earth, [Ents](https://en.wikipedia.org/wiki/Ent) are ancient tree-like beings
who serve as shepherds of the forest. Patient and deliberate, they tend to the trees under their care,
moving between them with purpose, ensuring each grows strong in its place. They are protectors and
caretakers, speaking the old languages, remembering what was and what must be preserved.

Like the Ents of old, **gent** watches over your worktrees—those parallel branches of your repository
that grow and develop in their own spaces. Where once you might have stumbled through tangled paths,
switching contexts with `git checkout` and losing your place, wt guides you between your working trees
with the steady wisdom of Treebeard himself. Each worktree stands as its own realm, isolated yet
connected to the greater forest of your repository. Gent knows them all by name, can walk you from one to
another, and tends to their creation and removal with the careful deliberation of one who has seen many
ages of growth. No longer must you uproot your current work to tend to another branch—simply walk between
the trees, and let wt shepherd your path.

## Getting Started

`gent` requires a [bare repository structure](https://git-scm.com/docs/gitrepository-layout/2.22.0) where
each branch is its own directory. `wt clone` sets this up automatically.

### Quick start

```bash
# Install gent
pip install thds.gent
# or with uv:
uv tool install thds.gent

# Set up shell integration (tab completion, directory navigation)
wt setup-shell

# Open a new terminal (or source your shell RC), then clone any repo
wt clone git@github.com:user/repo.git
```

Or as a one-liner without installing (requires [uv](https://docs.astral.sh/uv/)):

```bash
uvx --from thds.gent wt clone git@github.com:user/repo.git
```


### Already have a regular clone?

Consider cloning fresh with `wt clone` and deleting the old clone. If you want to convert your existing
repo in place, see [BARE_SETUP.md](BARE_SETUP.md).

### Shell integration

Navigation commands (`wt cd`, `wt co`, `wt root`) need shell integration to change your working
directory. Non-navigation commands (`wt clone`, `wt list`, `wt rm`, etc.) work without it.

```bash
wt setup-shell
```

This copies shell scripts to `~/.gent/`, adds tab completion, and configures your shell RC file. Safe to
re-run to update shell scripts after upgrading gent.

### Working with worktrees

```bash
wt list                    # See all worktrees
wt co feature/new-thing    # Create a new branch/worktree (auto-runs init)
wt cd main                 # Switch to main
wt start                   # Open editor
```

After setup, your repository uses a bare structure where each branch is a separate directory:

```
my-project/
├── .bare/          # Git metadata
├── main/           # Main branch/worktree
├── feature/foo/    # Feature branch/worktree
└── release/v2/     # Release branch/worktree
```

## Usage

### Commands

```bash
wt clone <url> [dir]   # Clone a repository with worktree structure
wt list                # List all worktrees
wt co <branch> [base]  # Checkout existing or create new branch as worktree
wt cd <branch>         # Navigate to a worktree
wt init [branch]       # Initialize worktree environment (.gent/init script)
wt start [branch]      # Run start hook (e.g. open editor)
wt rm <branch>         # Remove worktree and optionally delete branch
wt root                # Navigate to repository root
wt path <branch>       # Output path to a worktree (for scripting)
```

**How `wt co` works:**

The `wt co` command is smart about whether to checkout existing or create new:

1. If branch exists locally → checkout as worktree
1. If branch exists on remote → checkout and track remote
1. Otherwise → create new branch from base (current worktree, main, or specified)

### Common Workflows

**Checkout existing branch:**

```bash
wt co feature/existing          # Checkout existing branch (local or remote)
```

**Start a new feature:**

```bash
wt co feature/new-feature       # Create new from current worktree (or main)
wt co feature/new-feature main  # Create new from main explicitly
wt start                        # Open editor (optional)
```

`wt co` automatically runs `.gent/init` when creating a new worktree. Use `wt start` to launch your
editor or other "ready to work" actions.

**Switch between branches:**

```bash
wt cd main              # Jump to main
wt cd feature/testing   # Jump to feature
```

**Clean up:**

```bash
wt rm feature/old-branch         # Remove worktree and branch
wt rm feature/draft --force      # Force remove with uncommitted changes
```

## Shell Integration

Commands that change your shell's directory require special shell integration. The install script sets
this up automatically for bash, zsh, and xonsh:

- `wt cd <branch>` - Navigate to a worktree
- `wt co <branch>` - Create/checkout a worktree and navigate to it
- `wt root` - Navigate to repository root

For scripting where you need paths without changing directory, use `wt path <branch>`.

## Configuration

### Hook Scripts

gent supports hook scripts in the `.gent/` directory. Each hook is searched in precedence order:

1. `<worktree>/.gent/<hook>.local` — user, worktree-specific override
1. `<bare-root>/.gent/<hook>.local` — user, repo-specific override
1. `<worktree>/.gent/<hook>` — shared/team default

All hooks receive these environment variables:

- `GENT_WORKTREE_PATH`: Absolute path to the worktree
- `GENT_BRANCH`: Branch name

#### `.gent/init` — Worktree Initialization

Runs automatically when `wt co` creates a new worktree, and manually via `wt init`. Use this for
dependency installation and other setup that every worktree needs.

#### `.gent/start` — Start Working

Runs manually via `wt start`. Use this for actions you want on demand but not on every checkout — e.g.
opening an editor or IDE.

**Example hook:**

```python
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# ///
import os
import subprocess
from pathlib import Path

worktree_path = Path(os.environ["GENT_WORKTREE_PATH"])
print(f"Initializing {os.environ['GENT_BRANCH']}")

subprocess.run(["npm", "install"], cwd=worktree_path, check=True)
```

Make hooks executable: `chmod +x .gent/init`

## Troubleshooting

### "Shell integration not active" warning

If you see this warning when running `wt cd`:

```
⚠️  Shell integration not active - directory was NOT changed.
```

Run `wt setup-shell` to configure it, then restart your shell or run `source ~/.zshrc`.

### "Shell integration is outdated" warning

If you see this warning:

```
⚠️  wt shell integration is outdated (v1 → v2)
```

This means the Python package has been updated but the shell scripts in `~/.gent/` are from an older
version. Re-run:

```bash
wt setup-shell
```

## Developing

See [docs/DEVELOPING.md](docs/DEVELOPING.md) for architecture details and contribution guidelines.

## Uninstalling

```bash
# 1. Uninstall the tool
uv tool uninstall thds-gent

# 2. Remove shell integration
rm -rf ~/.gent

# 3. Remove from your shell RC file:
#    - ~/.zshrc or ~/.bashrc: delete the `[ -f ~/.gent/init.sh ] && source ...` line
#    - ~/.xonshrc: delete the `source ~/.gent/wt.xsh` block
```
