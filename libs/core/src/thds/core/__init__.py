"""Trilliant Health data science team core utils"""

# TODO: remove this comment - intended to kick off CI for full monorepo DAG

from importlib.metadata import PackageNotFoundError, version

from .meta import read_metadata

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = ""

metadata = read_metadata(__name__)
__basepackage__ = __name__
__commit__ = metadata.git_commit
