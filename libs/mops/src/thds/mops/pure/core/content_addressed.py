import typing as ty

from thds import humenc
from thds.core.hashing import Hash

from .uris import active_storage_root

B64_ADDRESSED = "{algo}-b64-addressed"
# we can save on storage and simplify lots of internals if we just
# hash all blobs and upload them to a key that is their hash.


def storage_content_addressed(hash_str: str, algo: str, storage_root: str = "") -> str:
    hash_namespace = B64_ADDRESSED.format(algo=algo)
    return f"{storage_root or active_storage_root()}/{hash_namespace}/{hash_str}"


class ContentAddressed(ty.NamedTuple):
    bytes_uri: str
    debug_uri: str


def wordybin_content_addressed(
    hash: Hash, storage_root: str = "", debug_name: str = ""
) -> ContentAddressed:
    """This should be used any time you have access to the raw bytes, so that we can stick
    with the Human Base 64 format.
    """
    base_uri = storage_content_addressed(humenc.encode(hash.bytes), hash.algo, storage_root)
    # corresponds with '_bytes' as used in `serialize_paths.py`
    return ContentAddressed(f"{base_uri}/_bytes", f"{base_uri}/{debug_name}" if debug_name else "")
