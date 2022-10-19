import hashlib
import os
import subprocess
import typing as ty
from contextlib import contextmanager
from pathlib import Path

from ...log import getLogger
from ...types import StrOrPath
from .constants import REPO_ROOT

LOGGER = getLogger(__name__)


@contextmanager
def stash_file(filename: str, stash_name: str) -> ty.Iterator[None]:
    """In the context of `with stash_file("a", "b"): ...`, the file named "a" will be renamed
    to "b". Upon leaving the context, the file named "a" will be restored to its original
    contents, and file "b" will be deleted. An exception is raised if "b" already exists."""

    try:
        os.rename(filename, stash_name)
    except FileNotFoundError as e:
        raise EnvironmentError(f"No such file: {filename}") from e
    except OSError as e:
        raise EnvironmentError(f"Please remove {stash_name}") from e

    try:
        yield
    finally:
        os.replace(stash_name, filename)


def find_repo_root() -> Path:
    path = Path.cwd()

    if path.name == REPO_ROOT:
        return path

    candidates = [parent for parent in path.parents if parent.name == REPO_ROOT]
    if not candidates:
        raise ValueError(f"Could not find '{REPO_ROOT}' on the path: '{path}'")
    elif len(candidates) > 1:
        raise ValueError(f"'{REPO_ROOT}' is ambiguous on the path: '{path}'")
    else:
        return candidates[0]


@contextmanager
def in_directory(dest: StrOrPath) -> ty.Iterator[None]:
    origin = Path().resolve()

    try:
        LOGGER.debug("Changing directory from '%s' -> '%s'.", origin, dest)
        os.chdir(dest)
        yield
    finally:
        LOGGER.debug("Changing directory back to '%s'.", origin)
        os.chdir(origin)


def git_changes() -> ty.Set[str]:
    cmd = ["git", "diff", "--name-only"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    if not proc.stdout:
        raise subprocess.CalledProcessError(
            returncode=1, cmd=" ".join(cmd), output="Could not get git changes from stdout."
        )

    changes = proc.stdout.readlines()
    return {change.decode("utf-8").strip() for change in changes}


def md5_file(path: StrOrPath) -> str:
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def md5_string(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()
