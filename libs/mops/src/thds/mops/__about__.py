from importlib.metadata import PackageNotFoundError, version

from thds.core import meta

__basepackage__ = __name__
try:
    __version__ = version("thds.mops")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "1.x"
__commit__ = meta.read_metadata(__name__).git_commit


def backward_compatible_with() -> int:
    return int(__version__.split(".")[0])


def feature_compatible_with() -> float:
    major, minor, _patch = __version__.split(".")
    return float(f"{major}.{minor}")
