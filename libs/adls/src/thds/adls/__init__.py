from thds import core

from . import abfss, defaults, etag, fqn, hashes, named_roots, source, source_tree, uri  # noqa: F401
from .cached import download_directory, download_to_cache, upload_through_cache  # noqa: F401
from .copy import copy_file, copy_files, wait_for_copy  # noqa: F401
from .errors import BlobNotFoundError  # noqa: F401
from .fqn import *  # noqa: F401,F403
from .global_client import get_global_client, get_global_fs_client  # noqa: F401
from .impl import *  # noqa: F401,F403
from .ro_cache import Cache, global_cache  # noqa: F401
from .upload import upload  # noqa: F401
from .uri import UriIsh, parse_any, parse_uri, resolve_any, resolve_uri  # noqa: F401

__version__ = core.meta.get_version(__name__)
metadata = core.meta.read_metadata(__name__)
__basepackage__ = __name__
__commit__ = metadata.git_commit

hashes.register_hashes()
# SPOOKY: without the above line, the hashing algorithms will not be registered with thds.core.hash_cache,
# which will be bad for core.Source as well as uploads and downloads.
