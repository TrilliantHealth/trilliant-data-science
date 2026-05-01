"""Drift catchers for thds.gent._repo.

The constants in `_repo.py` back URLs that get shown to users (error messages
in utils.py, the generated bare-repo README in readme.py). If a file
referenced here is renamed or moved without updating the constant, users
hit 404s. These tests fail first.
"""

from pathlib import Path

from thds.gent._repo import GENT_BARE_SETUP, GENT_README, GENT_REPO_PATH



def _libs_gent() -> Path:
    """Path to libs/gent on disk (this test file's grandparent.parent)."""
    return Path(__file__).parent.parent.parent


def _repo_root() -> Path:
    return _libs_gent().parent.parent


def test_gent_repo_path_matches_filesystem():
    assert (_repo_root() / GENT_REPO_PATH).resolve() == _libs_gent().resolve()


def test_referenced_gent_files_exist():
    libs_gent = _libs_gent()
    assert (libs_gent / GENT_README).is_file()
    assert (libs_gent / GENT_BARE_SETUP).is_file()


