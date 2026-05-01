"""
Tests for the wt setup-shell command.

These tests focus on the pure-ish functions that don't actually copy files
out of the installed package — _detect_shell_configs and the
_configure_posix_shell / _configure_xonsh idempotency contracts.
"""

from pathlib import Path

import pytest

from thds.gent.commands import setup_shell


@pytest.fixture
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Make Path.home() resolve to a temp directory for the duration of the test."""
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setattr(setup_shell, "GENT_DIR", tmp_path / ".gent")
    return tmp_path


def test_detect_shell_configs_prefers_zshrc(fake_home: Path):
    (fake_home / ".zshrc").write_text("# zsh\n")
    (fake_home / ".bashrc").write_text("# bash\n")
    posix, xonsh = setup_shell._detect_shell_configs()
    assert posix == fake_home / ".zshrc"
    assert xonsh is None


def test_detect_shell_configs_falls_back_to_bashrc(fake_home: Path):
    (fake_home / ".bashrc").write_text("# bash\n")
    posix, xonsh = setup_shell._detect_shell_configs()
    assert posix == fake_home / ".bashrc"
    assert xonsh is None


def test_detect_shell_configs_returns_xonsh_when_present(fake_home: Path):
    (fake_home / ".zshrc").write_text("# zsh\n")
    (fake_home / ".xonshrc").write_text("# xonsh\n")
    posix, xonsh = setup_shell._detect_shell_configs()
    assert posix == fake_home / ".zshrc"
    assert xonsh == fake_home / ".xonshrc"


def test_detect_shell_configs_returns_none_when_no_rc_files(fake_home: Path):
    posix, xonsh = setup_shell._detect_shell_configs()
    assert posix is None
    assert xonsh is None


def test_configure_posix_shell_appends_source_line(tmp_path: Path):
    rc = tmp_path / ".zshrc"
    rc.write_text("# user content\n")
    setup_shell._configure_posix_shell(rc)
    content = rc.read_text()
    assert "# user content" in content  # preserved
    assert setup_shell.POSIX_SOURCE_LINE in content


def test_configure_posix_shell_is_idempotent(tmp_path: Path):
    rc = tmp_path / ".zshrc"
    rc.write_text("# user content\n")
    setup_shell._configure_posix_shell(rc)
    first = rc.read_text()
    setup_shell._configure_posix_shell(rc)
    second = rc.read_text()
    assert first == second
    assert second.count(setup_shell.POSIX_SOURCE_LINE) == 1


def test_configure_xonsh_appends_source_block(tmp_path: Path):
    rc = tmp_path / ".xonshrc"
    rc.write_text("# user content\n")
    setup_shell._configure_xonsh(rc)
    content = rc.read_text()
    assert "# user content" in content
    assert ".gent/wt.xsh" in content


def test_configure_xonsh_is_idempotent(tmp_path: Path):
    rc = tmp_path / ".xonshrc"
    rc.write_text("# user content\n")
    setup_shell._configure_xonsh(rc)
    first = rc.read_text()
    setup_shell._configure_xonsh(rc)
    second = rc.read_text()
    assert first == second
    # XONSH_SOURCE_BLOCK contains ".gent/wt.xsh" twice (existence check + source).
    expected = setup_shell.XONSH_SOURCE_BLOCK.count(".gent/wt.xsh")
    assert second.count(".gent/wt.xsh") == expected


def test_init_sh_content_has_no_unescaped_unicode_literals():
    """Regression for the literal `\\u26a0\\ufe0f` bug — INIT_SH_CONTENT should
    embed the actual emoji bytes, not the escape sequence."""
    assert "\\u26a0" not in setup_shell.INIT_SH_CONTENT
    assert "\\ufe0f" not in setup_shell.INIT_SH_CONTENT
    assert "⚠️" in setup_shell.INIT_SH_CONTENT
