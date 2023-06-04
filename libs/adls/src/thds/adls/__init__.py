from thds.core import meta

from .fqn import *  # noqa: F401,F403
from .impl import *  # noqa: F401,F403
from .resource import AdlsHashedResource  # noqa: F401

__version__ = meta.get_version(__name__)
metadata = meta.read_metadata(__name__)
__basepackage__ = __name__
__commit__ = metadata.git_commit
