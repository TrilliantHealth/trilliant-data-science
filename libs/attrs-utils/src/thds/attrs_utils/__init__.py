from thds.core import meta

__version__ = meta.get_version(__name__)
__commit__ = meta.read_metadata(__name__).git_commit
