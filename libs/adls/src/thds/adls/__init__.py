from thds.core import meta

from .cached_up_down import download_to_cache, upload_through_cache  # noqa: F401
from .fqn import *  # noqa: F401,F403
from .impl import *  # noqa: F401,F403
from .ro_cache import Cache, global_cache  # noqa: F401

__version__ = meta.get_version(__name__)
metadata = meta.read_metadata(__name__)
__basepackage__ = __name__
__commit__ = metadata.git_commit
