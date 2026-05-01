## 1.3

- First public release on PyPI as `thds.gent`.
- Added `wt setup-shell` command so pip/uv users can configure shell integration without a source
  checkout. `install.sh` is now a thin editable-install wrapper that delegates to it.

## 1.2

- `wt co` now automatically runs `.gent/init` when creating a new worktree. Switching to an existing
  worktree does not trigger init. Init failures warn but don't fail the checkout.
- Added `wt start` command for on-demand actions like opening an editor. Uses the same hook precedence as
  `init` (worktree `.local` → bare-root `.local` → shared).
- Extracted shared hook-finding/running logic into `hooks.py` so `init`, `start`, and future hooks share
  the same machinery.

### 1.1.20260209

- Makes `wt clone` configure the bare repo created to enable remote branch fetching (which is
  [_not_ a default](https://git-scm.com/docs/git-clone#Documentation/git-clone.txt---bare)).
- Adds documentation for enabling remote branch fetching in BARE_SETUP.md.

## 1.1

- Added infrastructure versioning system to detect when shell scripts are out of sync with Python code.
  Users now see a warning when they need to re-run `install.sh`.
- Added shell wrapper detection for navigation commands (`cd`, `co`, `root`). When users run these
  commands without proper shell integration, they now see a helpful warning explaining that the directory
  was not changed and how to fix it.
- Added developer documentation in `docs/DEVELOPING.md` explaining the architecture and maintenance
  guidelines.

## 1.0

- Initial release with core worktree management commands: `clone`, `list`, `co`, `cd`, `init`, `rm`,
  `root`, `path`.
- Shell integration for directory-changing commands via `~/.gent/init.sh`.
- Tab completion support for bash and zsh.
- Repository initialization script support via `.gent/init`.
