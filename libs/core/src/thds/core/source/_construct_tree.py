import os
import typing as ty
from pathlib import Path

from ..files import path_from_uri
from ._construct import from_file
from .tree import SourceTree


def tree_from_directory(dirpath: ty.Union[str, os.PathLike]) -> SourceTree:
    path = path_from_uri(dirpath) if isinstance(dirpath, str) else Path(dirpath)

    if path.exists() and not path.is_dir():
        raise NotADirectoryError(f"File '{path}' is not a directory")
    if not path.exists():
        raise FileNotFoundError(f"Directory '{path}' not found")

    return SourceTree(sources=[from_file(file) for file in path.glob("**/*") if file.is_file()])
