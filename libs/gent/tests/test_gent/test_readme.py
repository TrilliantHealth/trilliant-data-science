"""
Tests for README.md generation at the bare repo root.
"""

import os
import time

from thds.gent._repo import GENT_README, GENT_REPO_PATH

from thds.gent.readme import _ONE_DAY_SECONDS, MANAGED_MARKER, ensure_readme, generate_readme_content

# --- generate_readme_content (pure function) ---


def test_generate_contains_managed_marker():
    content = generate_readme_content("my-repo")
    assert MANAGED_MARKER in content


def test_generate_contains_repo_name():
    content = generate_readme_content("my-repo")
    assert "# my-repo" in content


def test_generate_uses_default_branch_for_doc_links():
    content = generate_readme_content("repo", default_branch="main")
    assert f"main/{GENT_REPO_PATH}/{GENT_README}" in content


def test_generate_uses_custom_default_branch():
    content = generate_readme_content("repo", default_branch="develop")
    assert f"develop/{GENT_REPO_PATH}/{GENT_README}" in content
    # default_branch must be honored, not hardcoded
    assert f"/main/{GENT_REPO_PATH}" not in content


def test_generate_contains_llm_instructions():
    content = generate_readme_content("repo")
    assert "MUST use `wt` commands" in content
    assert "do NOT use raw" in content
    assert "should NOT be created independently" in content


def test_generate_contains_essential_commands():
    content = generate_readme_content("repo")
    assert "wt co" in content
    assert "wt rm" in content
    assert "wt list" in content
    assert "wt path" in content


def test_generate_mentions_wt_list_for_current_worktrees():
    content = generate_readme_content("repo")
    assert "`wt list`" in content


# --- ensure_readme (side-effecting) ---


def test_ensure_creates_readme_at_root(worktree_git_repo):
    ensure_readme(worktree_git_repo)

    readme_path = worktree_git_repo / "README.md"
    assert readme_path.exists()

    content = readme_path.read_text()
    assert MANAGED_MARKER in content


def test_ensure_does_not_overwrite_fresh_existing(worktree_git_repo):
    readme_path = worktree_git_repo / "README.md"
    readme_path.write_text("custom content")

    ensure_readme(worktree_git_repo)
    assert readme_path.read_text() == "custom content"


def test_ensure_refreshes_stale_readme(worktree_git_repo):
    readme_path = worktree_git_repo / "README.md"
    readme_path.write_text("old content")

    # Set mtime to 2 days ago
    old_time = time.time() - (_ONE_DAY_SECONDS * 2)
    os.utime(readme_path, (old_time, old_time))

    ensure_readme(worktree_git_repo)
    content = readme_path.read_text()
    assert MANAGED_MARKER in content
    assert content != "old content"


def test_ensure_does_not_refresh_recent_readme(worktree_git_repo):
    readme_path = worktree_git_repo / "README.md"
    readme_path.write_text("recent content")

    # Set mtime to 1 hour ago (well within 1 day)
    recent_time = time.time() - 3600
    os.utime(readme_path, (recent_time, recent_time))

    ensure_readme(worktree_git_repo)
    assert readme_path.read_text() == "recent content"


def test_ensure_created_by_wt_co(worktree_git_repo, run_wt):
    main_path = worktree_git_repo / "main"

    # README should be created as a side effect of co
    run_wt("co", ["feature/readme-test"], cwd=main_path)

    readme_path = worktree_git_repo / "README.md"
    assert readme_path.exists()
    content = readme_path.read_text()
    assert MANAGED_MARKER in content


def test_ensure_never_raises_on_invalid_path(tmp_path):
    invalid_path = tmp_path / "nonexistent"
    ensure_readme(invalid_path)


def test_ensure_never_raises_on_non_git_directory(tmp_path):
    ensure_readme(tmp_path)
