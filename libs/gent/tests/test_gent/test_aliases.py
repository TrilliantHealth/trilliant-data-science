"""Tests for command aliases (`wt sw` -> `wt co`)."""

from thds.gent.__main__ import ALIASES, COMMANDS, aliases_for


def test_every_alias_targets_a_real_command():
    assert not {target for target in ALIASES.values() if target not in COMMANDS}


def test_no_alias_shadows_a_command():
    assert not set(ALIASES) & set(COMMANDS)


def test_aliases_for_finds_the_alias():
    assert aliases_for("co") == ["sw"]


def test_aliases_for_is_empty_when_there_are_none():
    assert aliases_for("list") == []


def test_sw_creates_a_worktree_like_co(worktree_git_repo, run_wt):
    result = run_wt("sw", ["feature/via-alias"], cwd=worktree_git_repo / "main")

    assert result.returncode == 0
    assert (worktree_git_repo / "feature" / "via-alias").is_dir()


def test_help_lists_the_alias_alongside_its_target(worktree_git_repo, run_wt):
    result = run_wt("--help", [], cwd=worktree_git_repo / "main")

    assert "co (sw)" in result.stdout
    assert "\n  sw " not in result.stdout


def test_completion_offers_aliases(worktree_git_repo, run_completion):
    result = run_completion("--subcommands", cwd=worktree_git_repo / "main")

    assert "sw" in result.stdout.split()


def test_completion_descriptions_name_the_target(worktree_git_repo, run_completion):
    lines = run_completion(
        "--subcommands", "--with-descriptions", cwd=worktree_git_repo / "main"
    ).stdout.splitlines()

    assert "sw\tAlias for 'co'" in lines
    assert len([line for line in lines if line.startswith("co\t")]) == 1
