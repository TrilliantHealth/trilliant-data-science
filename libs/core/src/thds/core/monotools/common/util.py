import os
import typing as ty
from contextlib import contextmanager
from pathlib import Path

from ...types import StrOrPath
from .constants import REPO_ROOT


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


def path_from_repo(path: StrOrPath) -> str:
    path = Path(path) if not isinstance(path, Path) else path
    _, repo_relative_path = str(path.resolve()).split(REPO_ROOT)

    return os.path.join(REPO_ROOT, repo_relative_path)
