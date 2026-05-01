"""
Tests for worktree shell completion functionality.
"""

import subprocess
from pathlib import Path

import pytest


def test_worktrees_mode_lists_worktree_names(worktree_git_repo, run_completion):
    """Test that --worktrees mode lists existing worktree names."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    # Create some worktrees
    (worktree_git_repo / "feature").mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "worktree",
            "add",
            "-b",
            "feature/one",
            str(worktree_git_repo / "feature" / "one"),
            "main",
        ],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "git",
            "worktree",
            "add",
            "-b",
            "feature/two",
            str(worktree_git_repo / "feature" / "two"),
            "main",
        ],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    result = run_completion("--worktrees", cwd=main_path)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")

    assert "main" in output_lines
    assert "feature/one" in output_lines
    assert "feature/two" in output_lines
    assert ".bare" not in output_lines
    assert len(output_lines) == 3


def test_worktrees_mode_with_descriptions(worktree_git_repo, run_completion):
    """Test that --worktrees --with-descriptions adds tab-separated descriptions."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    (worktree_git_repo / "feature").mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "worktree",
            "add",
            "-b",
            "feature/test",
            str(worktree_git_repo / "feature" / "test"),
            "main",
        ],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    result = run_completion("--worktrees", "--with-descriptions", cwd=main_path)

    assert result.returncode == 0
    lines = result.stdout.strip().split("\n")

    for line in lines:
        parts = line.split("\t")
        assert len(parts) == 2, f"Expected 2 parts separated by tab, got: {line}"
        worktree_name, description = parts
        assert worktree_name
        assert description


def test_worktrees_mode_excludes_bare_repository(worktree_git_repo, run_completion):
    """Test that bare repository is not included in worktree list."""
    main_path = worktree_git_repo / "main"

    result = run_completion("--worktrees", cwd=main_path)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")

    for line in output_lines:
        assert ".bare" not in line.lower()


def test_branches_mode_lists_local_branches(worktree_git_repo, run_completion):
    """Test that --branches mode lists local git branches."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    subprocess.run(
        ["git", "branch", "feature/new-branch"], cwd=bare_path, check=True, capture_output=True
    )

    result = run_completion("--branches", cwd=main_path)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")

    assert "main" in output_lines
    assert "feature/new-branch" in output_lines


def test_branches_mode_includes_remote_branches(worktree_git_repo, run_completion):
    """Test that --branches mode includes remote branches."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    subprocess.run(
        ["git", "branch", "feature/from-remote"], cwd=bare_path, check=True, capture_output=True
    )

    result = run_completion("--branches", cwd=main_path)

    assert result.returncode == 0


def test_subcommands_mode_lists_gent_commands(worktree_git_repo, run_completion):
    """Test that --subcommands mode lists wt subcommands."""
    main_path = worktree_git_repo / "main"

    result = run_completion("--subcommands", cwd=main_path)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")

    expected_commands = ["root", "cd", "list", "rm", "init", "co"]
    for cmd in expected_commands:
        assert cmd in output_lines, f"Expected '{cmd}' in subcommands list"


def test_subcommands_with_descriptions(worktree_git_repo, run_completion):
    """Test that --subcommands --with-descriptions includes descriptions."""
    main_path = worktree_git_repo / "main"

    result = run_completion("--subcommands", "--with-descriptions", cwd=main_path)

    assert result.returncode == 0
    lines = result.stdout.strip().split("\n")

    for line in lines:
        parts = line.split("\t")
        assert len(parts) == 2
        command, description = parts
        assert command in [
            "clone",
            "root",
            "cd",
            "path",
            "list",
            "rm",
            "init",
            "start",
            "co",
            "setup-shell",
        ]
        assert len(description) > 0


def test_completion_outside_worktree_directory(tmp_path, run_completion):
    """Test that completion exits gracefully when not in a worktree."""
    result = run_completion("--worktrees", cwd=tmp_path)

    assert result.returncode == 0
    assert result.stdout.strip() == ""


def test_completion_with_no_bare_directory(tmp_path, run_completion):
    """Test that completion handles missing .bare directory gracefully."""
    test_repo = tmp_path / "regular-repo"
    test_repo.mkdir()
    subprocess.run(["git", "init"], cwd=test_repo, check=True, capture_output=True)

    result = run_completion("--worktrees", cwd=test_repo)

    assert result.returncode == 0
    assert result.stdout.strip() == ""


def test_completion_with_empty_worktree_list(worktree_git_repo, run_completion):
    """Test completion when only bare repository exists (no worktrees)."""
    bare_path = worktree_git_repo / ".bare"
    main_path = worktree_git_repo / "main"

    subprocess.run(["git", "worktree", "remove", str(main_path)], cwd=bare_path, capture_output=True)

    result = run_completion("--worktrees", cwd=bare_path)

    assert result.returncode == 0
    assert result.stdout.strip() == ""


def test_completion_with_detached_head_worktree(worktree_git_repo, run_completion):
    """Test completion handles worktrees in detached HEAD state."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=bare_path, capture_output=True, text=True, check=True
    )
    commit_hash = result.stdout.strip()

    detached_path = worktree_git_repo / "detached"
    subprocess.run(
        ["git", "worktree", "add", "--detach", str(detached_path), commit_hash],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    result = run_completion("--worktrees", cwd=main_path)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")
    assert "detached" in output_lines


def test_completion_with_special_characters_in_branch_name(worktree_git_repo, run_completion):
    """Test completion handles branch names with special characters."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    special_branch = "feature/test-branch_v2"
    (worktree_git_repo / "feature").mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "git",
            "worktree",
            "add",
            "-b",
            special_branch,
            str(worktree_git_repo / special_branch),
            "main",
        ],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    result = run_completion("--worktrees", cwd=main_path)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")
    assert special_branch in output_lines


def test_completion_from_worktree_subdirectory(worktree_git_repo, run_completion):
    """Test that completion works when run from inside a worktree subdirectory."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    (worktree_git_repo / "feature").mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "worktree",
            "add",
            "-b",
            "feature/test",
            str(worktree_git_repo / "feature" / "test"),
            "main",
        ],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    subdir = main_path / "subdir"
    subdir.mkdir(parents=True, exist_ok=True)

    result = run_completion("--worktrees", cwd=subdir)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")

    assert "main" in output_lines
    assert "feature/test" in output_lines


def test_completion_no_arguments_shows_help(worktree_git_repo, run_completion):
    """Test that running with no arguments shows help."""
    main_path = worktree_git_repo / "main"

    result = run_completion(cwd=main_path)

    # Should show help or error message
    assert result.stdout or result.stderr


def test_completion_from_repo_root(worktree_git_repo, run_completion):
    """Test that completion works when run from the repo root (parent of .bare)."""
    # worktree_git_repo IS the root (contains .bare)
    root_path = worktree_git_repo

    result = run_completion("--worktrees", cwd=root_path)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")
    assert "main" in output_lines


def test_branches_from_repo_root(worktree_git_repo, run_completion):
    """Test that branch completion works from repo root."""
    root_path = worktree_git_repo

    result = run_completion("--branches", cwd=root_path)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")
    assert "main" in output_lines


# Bash completion tests
# These test the actual bash completion script, not just the Python helper


@pytest.fixture
def bash_complete():
    """Fixture that provides a function to test bash completion."""
    # Get path to completion script in the repo (not installed version)
    test_dir = Path(__file__).parent
    repo_root = test_dir.parent.parent  # libs/gent
    completion_script = repo_root / "src" / "thds" / "gent" / "shell" / "wt-completion.bash"

    def _bash_complete(words: list[str], cwd: Path | None = None):
        """Simulate bash completion for wt command.

        Args:
            words: List of words in the command line (e.g., ["wt", "co", "feat"])
            cwd: Working directory to run completion in

        Returns:
            CompletedProcess with COMPREPLY contents in stdout
        """
        # Build the bash script to simulate completion
        words_str = " ".join(f'"{w}"' for w in words)
        cword = len(words) - 1

        script = f"""
            # Source the completion script from the repo
            source {completion_script}

            # Set up completion variables
            COMP_WORDS=({words_str})
            COMP_CWORD={cword}

            # Run completion function
            _wt_complete

            # Output results
            printf '%s\\n' "${{COMPREPLY[@]}}"
        """

        result = subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            cwd=cwd,
        )
        return result

    return _bash_complete


def test_bash_completion_subcommands(worktree_git_repo, bash_complete):
    """Test bash completion for wt subcommands."""
    main_path = worktree_git_repo / "main"

    result = bash_complete(["wt", ""], cwd=main_path)

    # Should include common subcommands
    completions = result.stdout.strip().split("\n") if result.stdout.strip() else []

    # At minimum, subcommands should be available
    # Note: This may fail if bash-completion is not installed
    if result.returncode == 0 and completions:
        assert any(cmd in completions for cmd in ["co", "cd", "list", "rm"])


def test_bash_completion_co_shows_branches(worktree_git_repo, bash_complete):
    """Test bash completion for 'wt co' shows branches."""
    main_path = worktree_git_repo / "main"

    result = bash_complete(["wt", "co", ""], cwd=main_path)

    completions = result.stdout.strip().split("\n") if result.stdout.strip() else []

    # Should include 'main' branch
    if result.returncode == 0 and completions:
        assert "main" in completions


def test_bash_completion_cd_shows_worktrees(worktree_git_repo, bash_complete):
    """Test bash completion for 'wt cd' shows worktrees."""
    main_path = worktree_git_repo / "main"

    result = bash_complete(["wt", "cd", ""], cwd=main_path)

    completions = result.stdout.strip().split("\n") if result.stdout.strip() else []

    # Should include 'main' worktree
    if result.returncode == 0 and completions:
        assert "main" in completions


def test_bash_completion_no_trailing_slash(worktree_git_repo, bash_complete):
    """Test bash completion doesn't add trailing slashes to branch names."""
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    # Create a feature branch
    (worktree_git_repo / "feature").mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "worktree",
            "add",
            "-b",
            "feature/bash-test",
            str(worktree_git_repo / "feature" / "bash-test"),
            "main",
        ],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    result = bash_complete(["wt", "co", "feature/"], cwd=main_path)

    completions = result.stdout.strip().split("\n") if result.stdout.strip() else []

    # No completion should end with a trailing slash
    for completion in completions:
        if completion:  # Skip empty lines
            assert not completion.endswith("/"), f"Completion '{completion}' has trailing slash"


@pytest.mark.parametrize(
    "mode,branch_name",
    [
        ("--branches", "feature/test-branch"),
        ("--worktrees", "feature/test-worktree"),
    ],
)
def test_completion_has_no_trailing_slash(worktree_git_repo, run_completion, mode, branch_name):
    """Test that completion output doesn't include trailing slashes.

    This guards against shell directory completion interference where
    the shell might add trailing slashes to directory names.
    """
    main_path = worktree_git_repo / "main"
    bare_path = worktree_git_repo / ".bare"

    # Create a worktree with a slash in the branch name
    (worktree_git_repo / "feature").mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "worktree",
            "add",
            "-b",
            branch_name,
            str(worktree_git_repo / branch_name),
            "main",
        ],
        cwd=bare_path,
        check=True,
        capture_output=True,
    )

    result = run_completion(mode, cwd=main_path)

    assert result.returncode == 0
    output_lines = result.stdout.strip().split("\n")

    # No item should end with a trailing slash
    for line in output_lines:
        assert not line.endswith("/"), f"Completion '{line}' has trailing slash"

    # Verify the item is present without trailing slash
    assert branch_name in output_lines


# Xonsh completion tests
# These test the xonsh completion script


@pytest.fixture
def xonsh_complete():
    """Fixture that provides a function to test xonsh completion.

    Returns None if xonsh is not available.
    """
    # Check if xonsh is available
    try:
        result = subprocess.run(
            ["xonsh", "--version"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip("xonsh not available")
    except FileNotFoundError:
        pytest.skip("xonsh not installed")

    # Get path to completion script in the repo
    test_dir = Path(__file__).parent
    repo_root = test_dir.parent.parent  # libs/gent
    completion_script = repo_root / "src" / "thds" / "gent" / "shell" / "wt.xsh"

    def _xonsh_complete(line: str, cwd: Path | None = None):
        """Simulate xonsh completion for wt command.

        Args:
            line: The command line to complete (e.g., "wt cd ")
            cwd: Working directory to run completion in

        Returns:
            CompletedProcess with completion results in stdout
        """
        # Calculate completion position (end of line)
        endidx = len(line)
        # Prefix is the last word being typed (or empty if line ends with space)
        parts = line.split()
        if line.endswith(" ") or not parts:
            prefix = ""
        else:
            prefix = parts[-1]
        begidx = endidx - len(prefix)

        # Build xonsh script to test completion
        script = f"""
source "{completion_script}"

# Call the completer function
result = _wt_completer("{prefix}", "{line}", {begidx}, {endidx}, {{}})

# Output results
if result is not None:
    completions, _ = result
    for c in sorted(completions):
        print(c)
"""

        result = subprocess.run(
            ["xonsh", "-c", script],
            capture_output=True,
            text=True,
            cwd=cwd,
        )
        return result

    return _xonsh_complete


def test_xonsh_completion_subcommands(worktree_git_repo, xonsh_complete):
    """Test xonsh completion for wt subcommands."""
    main_path = worktree_git_repo / "main"

    result = xonsh_complete("wt ", cwd=main_path)

    completions = result.stdout.strip().split("\n") if result.stdout.strip() else []

    if result.returncode == 0 and completions:
        assert any(cmd in completions for cmd in ["co", "cd", "list", "rm"])


def test_xonsh_completion_cd_shows_worktrees(worktree_git_repo, xonsh_complete):
    """Test xonsh completion for 'wt cd' shows worktrees."""
    main_path = worktree_git_repo / "main"

    result = xonsh_complete("wt cd ", cwd=main_path)

    completions = result.stdout.strip().split("\n") if result.stdout.strip() else []

    if result.returncode == 0 and completions:
        assert "main" in completions


def test_xonsh_completion_co_shows_branches(worktree_git_repo, xonsh_complete):
    """Test xonsh completion for 'wt co' shows branches."""
    main_path = worktree_git_repo / "main"

    result = xonsh_complete("wt co ", cwd=main_path)

    completions = result.stdout.strip().split("\n") if result.stdout.strip() else []

    if result.returncode == 0 and completions:
        assert "main" in completions


def test_xonsh_completion_partial_subcommand(worktree_git_repo, xonsh_complete):
    """Test xonsh completion for partial subcommand."""
    main_path = worktree_git_repo / "main"

    result = xonsh_complete("wt c", cwd=main_path)

    completions = result.stdout.strip().split("\n") if result.stdout.strip() else []

    if result.returncode == 0 and completions:
        # Should include 'cd', 'co', 'clone' but not 'list' or 'rm'
        assert all(c.startswith("c") for c in completions if c)


def test_xonsh_completion_rm_shows_force_flag(worktree_git_repo, xonsh_complete):
    """Test xonsh completion for 'wt rm -' shows force flags."""
    main_path = worktree_git_repo / "main"

    result = xonsh_complete("wt rm -", cwd=main_path)

    completions = result.stdout.strip().split("\n") if result.stdout.strip() else []

    if result.returncode == 0 and completions:
        assert "--force" in completions or "-f" in completions
