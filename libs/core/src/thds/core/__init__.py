"""Trilliant Health data science team core utils"""
import sys
from importlib.metadata import PackageNotFoundError, version

from .meta import read_metadata

try:
    __version__ = version(__name__)
    assert __version__, f"if package {__name__} is found, version should exist."
except PackageNotFoundError:  # pragma: no cover
    sys.stderr.write(f"no version found for {__name__}")
    try:
        under_name = __name__.replace(".", "_")
        __version__ = version(under_name)
    except PackageNotFoundError:
        sys.stderr.write(f"no version found for {under_name}")
        __version__ = ""

metadata = read_metadata(__name__)
__basepackage__ = __name__
__commit__ = metadata.git_commit
