"""Wrap openable, read-only data that is either locally-present or downloadable,

yet will not be downloaded (if non-local) until it is actually opened or unwrapped.
"""

from . import serde, tree  # noqa: F401
from ._construct import set_file_autohash  # noqa: F401
from ._construct import from_file, from_uri, path_from_uri, register_from_uri_handler  # noqa: F401
from ._construct_tree import tree_from_directory  # noqa: F401
from ._download import Downloader, register_download_handler  # noqa: F401
from .src import Source  # noqa: F401
