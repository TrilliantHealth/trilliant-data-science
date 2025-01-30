from thds.core import meta

__version__ = meta.get_version("thds.mops")
__commit__ = meta.read_metadata(__name__).git_commit


def backward_compatible_with() -> int:
    return 2  # v2 is the current major version.
