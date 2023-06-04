from thds.core import meta

__basepackage__ = __name__
__version__ = meta.get_version("thds.mops")
__commit__ = meta.read_metadata(__name__).git_commit


def backward_compatible_with() -> int:
    try:
        return int(__version__.split(".")[0])
    except ValueError:
        print(f"Unable to parse version <{__version__}>; assuming major version is 1")
        return 1


def feature_compatible_with() -> float:
    major, minor, _patch = __version__.split(".")
    return float(f"{major}.{minor}")
