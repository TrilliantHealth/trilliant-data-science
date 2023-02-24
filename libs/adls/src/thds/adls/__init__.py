from thds.core import meta

from .impl import *  # noqa: F401,F403

__version__ = meta.get_version(__name__)
metadata = meta.read_metadata(__name__)
__basepackage__ = __name__
__commit__ = metadata.git_commit
