from importlib.metadata import PackageNotFoundError, version

from thds.core import meta

from .impl import *  # noqa: F401,F403

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = ""

metadata = meta.read_metadata(__name__)
__basepackage__ = __name__
__commit__ = metadata.git_commit
