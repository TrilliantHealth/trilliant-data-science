# Manual Bare Repository Setup

`wt` requires a bare repository structure with worktrees. The easiest way to set this up is with
`wt clone` (see README). If you need to manually convert an existing repository or clone without `wt`,
follow the steps below.

**What you're creating:** A directory structure where `.bare/` contains the git metadata and each branch
is checked out in its own subdirectory (e.g., `main/`, `feature/foo/`).

**Learn about git worktrees:**

- [Git Worktree Documentation](https://git-scm.com/docs/git-worktree) - Official reference
- [Atlassian Git Worktree Tutorial](https://www.atlassian.com/git/tutorials/git-worktree) - Guide with
  examples

## Convert an Existing Repository

If you already have a cloned repository:

```bash
# 1. Ensure clean working directory
cd ~/repos/my-project
git status  # Commit or stash any changes first

# 2. Note current branch
CURRENT_BRANCH=$(git branch --show-current)

# 3. Restructure
cd ..
mv my-project my-project-backup
mkdir my-project
cd my-project

# 4. Move .git to .bare and configure
mv ../my-project-backup/.git .bare
git --git-dir=.bare config --bool core.bare true
git --git-dir=.bare config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*"  # enables fetching remote branches

# 5. Create worktree and restore files
git --git-dir=.bare worktree add "$CURRENT_BRANCH" "$CURRENT_BRANCH"
mv ../my-project-backup/* "$CURRENT_BRANCH"/ 2>/dev/null || true
mv ../my-project-backup/.* "$CURRENT_BRANCH"/ 2>/dev/null || true
rm -rf ../my-project-backup

# 6. Verify
ls -la  # Should show: .bare/  main/ (or your branch name)
cd "$CURRENT_BRANCH" && wt list
```

## Clone a Repository as Bare

For fresh clones, use `wt clone` which handles all of this automatically:

```bash
# If gent is installed:
wt clone git@github.com:user/repo.git

# Or as a one-liner without installing:
uvx --from thds.gent wt clone git@github.com:user/repo.git
```

## Initialize a New Repository

If you're starting a new project from scratch:

```bash
cd ~/repos
mkdir my-project && cd my-project

# Initialize bare repo
git init --bare .bare

# Enable fetching remote branches
git --git-dir=.bare config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*"

# Create main worktree
git --git-dir=.bare worktree add main
cd main

# Configure and create initial commit
git config user.email "you@example.com"
git config user.name "Your Name"
echo "# My Project" > README.md
git add README.md
git commit -m "Initial commit"

# Verify
cd .. && wt list
```
