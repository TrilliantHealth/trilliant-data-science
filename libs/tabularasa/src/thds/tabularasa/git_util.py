import contextlib
import os
import subprocess
import typing as ty
from pathlib import Path

from thds.core import git

StrOrPath = ty.Union[str, os.PathLike]


def relative_to_root(path: StrOrPath) -> Path:
    """Path relative to the repo root. Can be given either as an absolute path or relative path, in
    which case it's assumed to be relative to the current working directory. Note: paths which have
    already been relativized to the repo root should *not* be passed here - that will only be correct
    if the working directory *is* the repo root"""
    return Path(path).resolve().relative_to(git.get_repo_root())


@contextlib.contextmanager
def _subcap(cmd: list, **kwargs) -> ty.Iterator:
    try:
        yield
    except subprocess.CalledProcessError as cpe:
        print("stdout:", cpe.stdout)
        print("stderr:", cpe.stderr)
        print("Failed; retrying: " + " ".join(cmd))
        subprocess.run(cmd, check=True)


def blob_contents(path: StrOrPath, ref: str) -> bytes:
    """Read the text contents of a specific file (relative to the repo root) at a specific git ref.
    Note that git *requires* the paths given here to be relative to the repo root"""
    cmd = ["git", "show", f"{ref}:{relative_to_root(path)}"]
    with _subcap(cmd):
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=True)
    return proc.stdout
