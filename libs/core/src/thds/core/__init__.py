"""Trilliant Health data science team core utils"""
from .meta import get_version, read_metadata

__version__ = get_version(__name__)
metadata = read_metadata(__name__)
__basepackage__ = __name__
__commit__ = metadata.git_commit
