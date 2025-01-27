from thds.core import meta

from . import abfss, defaults, etag, fqn, named_roots, resource, source, uri  # noqa: F401
from .cached_up_down import download_directory, download_to_cache, upload_through_cache  # noqa: F401
from .errors import BlobNotFoundError  # noqa: F401
from .fqn import *  # noqa: F401,F403
from .global_client import get_global_client, get_global_fs_client  # noqa: F401
from .impl import *  # noqa: F401,F403
from .ro_cache import Cache, global_cache  # noqa: F401
from .uri import UriIsh, parse_any, parse_uri, resolve_any, resolve_uri  # noqa: F401

__version__ = meta.get_version(__name__)
metadata = meta.read_metadata(__name__)
__basepackage__ = __name__
__commit__ = metadata.git_commit
