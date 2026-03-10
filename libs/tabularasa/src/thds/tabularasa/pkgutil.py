"""Replacements for pkg_resources functions using importlib.resources.

All functions accept a dotted package name and a forward-slash-separated
resource path, matching the calling conventions of the old pkg_resources API.
"""

import importlib.abc
import importlib.resources
import typing as ty
from pathlib import Path


def _traversable(package: str, resource_path: str) -> importlib.abc.Traversable:
    t = importlib.resources.files(package)
    # in 3.11+ you can just do `t / resource_path`, but in 3.10 and below you have to split the path into parts
    for part in resource_path.split("/"):
        if part:
            t = t.joinpath(part)
    return t


def resource_filename(package: str, resource_path: str) -> str:
    """Filesystem path to a package resource.

    Only works for resources backed by actual files on disk (editable installs, normal filesystem packages).
    """
    t = _traversable(package, resource_path)
    assert isinstance(t, Path), (
        f"{resource_filename.__qualname__} only works for editable installs or packages on the normal filesystem; {package!r} is not supported"
    )
    return str(t)


def resource_stream(package: str, resource_path: str) -> ty.IO[bytes]:
    return _traversable(package, resource_path).open("rb")


def resource_exists(package: str, resource_path: str) -> bool:
    """Raises ModuleNotFoundError if the package itself isn't installed."""
    t = _traversable(package, resource_path)
    return t.is_file() or t.is_dir()


def resource_isdir(package: str, resource_path: str) -> bool:
    return _traversable(package, resource_path).is_dir()


def resource_listdir(package: str, resource_path: str) -> list[str]:
    return [item.name for item in _traversable(package, resource_path).iterdir()]
