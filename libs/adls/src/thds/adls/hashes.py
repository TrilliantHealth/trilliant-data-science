import contextlib
import os
import sys
import typing as ty
from functools import partial

import xxhash

from thds.core import hash_cache, hashing, log, source, types
from thds.core.hashing import Hash, SomehowReadable

from . import errors, file_properties
from ._etag import ETAG_FAKE_HASH_NAME, add_to_etag_cache, extract_etag_bytes, hash_file_fake_etag
from .fqn import AdlsFqn

logger = log.getLogger(__name__)

_KNOWN_METADATA_ALGOS: ty.Final = ("xxh3_128", "blake3")  # in order of descending preference
PREFERRED_ALGOS: ty.Final = _KNOWN_METADATA_ALGOS[:1]
assert PREFERRED_ALGOS == ("xxh3_128",)
AnyStrSrc = ty.Union[SomehowReadable, ty.Iterable[ty.AnyStr]]
# this type closely corresponds to what the underlying DataLakeStorageClient will accept for upload_data.


def default_hasher() -> hashing.Hasher:
    return xxhash.xxh3_128()


def _xxhash_hasher(algo: str) -> hashing.Hasher:
    return getattr(xxhash, algo)()


def register_hashes():
    for algo in xxhash.algorithms_available:
        hashing.add_named_hash(algo, _xxhash_hasher)
    source.set_file_autohash(PREFERRED_ALGOS[0], _xxhash_hasher)

    try:
        from blake3 import blake3

        hashing.add_named_hash("blake3", lambda _: blake3())  # type: ignore
    except ModuleNotFoundError:
        pass


def _hash_path_if_exists(
    file_hasher: ty.Callable[[types.StrOrPath], hashing.Hash], path: types.StrOrPath
) -> ty.Optional[hashing.Hash]:
    if not path or not os.path.exists(path):  # does not exist if it's a symlink with a bad referent.
        return None
    return file_hasher(path)


def hash_path_for_algo(
    algo: str,
) -> ty.Callable[[types.StrOrPath], ty.Optional[hashing.Hash]]:
    """Return a function that hashes a path for the given algorithm."""
    if algo == ETAG_FAKE_HASH_NAME:
        return hash_file_fake_etag

    return partial(_hash_path_if_exists, partial(hash_cache.filehash, algo))


def metadata_hash_b64_key(algo: str) -> str:
    return f"hash_{algo}_b64"


def extract_hashes_from_metadata(metadata: dict) -> ty.Iterable[hashing.Hash]:
    # NOTE! the order here is critical, because we want to _prefer_ the faster hash if it exists.
    for hash_algo in _KNOWN_METADATA_ALGOS:
        md_key = metadata_hash_b64_key(hash_algo)
        if metadata and md_key in metadata:
            yield hashing.Hash(hash_algo, hashing.db64(metadata[md_key]))


def extract_hashes_from_props(
    props: ty.Optional[file_properties.PropertiesP],
) -> dict[str, hashing.Hash]:
    if not props:
        return dict()

    hashes = list(extract_hashes_from_metadata(props.metadata or dict()))
    if props.content_settings and props.content_settings.content_md5:
        hashes.append(hashing.Hash("md5", bytes(props.content_settings.content_md5)))

    if props.etag:
        # this is the final fallback. it cannot be checked locally, but at least
        # it can be checked against what exists remotely the next time we want to use it.
        if etag_bytes := extract_etag_bytes(props.etag):
            hashes.append(hashing.Hash(sys.intern(ETAG_FAKE_HASH_NAME), etag_bytes))

    return {h.algo: h for h in hashes}


@contextlib.contextmanager
def verify_hashes_before_and_after_download(
    remote_hash: ty.Optional[Hash],
    expected_hash: ty.Optional[Hash],
    fqn: AdlsFqn,
    local_dest: types.StrOrPath,
) -> ty.Iterator[None]:
    if remote_hash and expected_hash and remote_hash != expected_hash:
        raise errors.HashMismatchError(
            f"ADLS thinks the {remote_hash.algo} of {fqn} is {hashing.b64(remote_hash.bytes)},"
            f" but we expected {hashing.b64(expected_hash.bytes)}."
            " This may indicate that we need to update a hash in the codebase."
        )

    yield  # perform download

    expected_algo = expected_hash.algo if expected_hash else None
    if not expected_algo and remote_hash:
        expected_algo = remote_hash.algo

    if not expected_algo:
        # if we have neither a user-provided hash nor a remotely-found hash, then we have nothing to check.
        return

    assert expected_hash or remote_hash, "At least one of expected or remote hash must be present."
    with log.logger_context(hash_for="after-download"):
        if expected_algo == ETAG_FAKE_HASH_NAME:
            assert remote_hash, f"An Etag hash should always originate remotely: {fqn}"
            local_hash = add_to_etag_cache(local_dest, remote_hash.bytes)
        else:
            local_hash = hash_cache.filehash(expected_algo, local_dest)

    if remote_hash and remote_hash != local_hash:
        raise errors.HashMismatchError(
            f"The {local_hash.algo} of the downloaded file {local_dest} is {hashing.b64(local_hash.bytes)},"
            f" but the remote ({fqn}) says it should be {hashing.b64(remote_hash.bytes)}."
            f" This may indicate that ADLS has an erroneous {remote_hash.algo} for {fqn}."
        )

    if expected_hash and local_hash != expected_hash:
        raise errors.HashMismatchError(
            f"The {local_hash.algo} of the downloaded file {local_dest} is {hashing.b64(local_hash.bytes)},"
            f" but we expected it to be {hashing.b64(expected_hash.bytes)}."
            f" This probably indicates a corrupted download of {fqn}"
        )

    all_hashes = dict(local=local_hash, remote=remote_hash, expected=expected_hash)
    real_hashes = list(filter(None, all_hashes.values()))
    assert len(real_hashes) > 0, all_hashes
    assert all(real_hashes[0] == h for h in real_hashes), all_hashes


def metadata_hash_dict(hash: Hash) -> dict[str, str]:
    return {metadata_hash_b64_key(hash.algo): hashing.b64(hash.bytes)}


def create_hash_metadata_if_missing(
    file_properties: ty.Optional[file_properties.FileProperties], new_hash: ty.Optional[Hash]
) -> dict:
    if not (file_properties and new_hash):
        # without file properties, we can't match the etag when we try to set this.
        return dict()

    if new_hash.algo == ETAG_FAKE_HASH_NAME:
        # we never want to write etag-based hashes into metadata.
        return dict()

    existing_metadata = file_properties.metadata or dict()
    if metadata_hash_b64_key(new_hash.algo) not in existing_metadata:
        return {**existing_metadata, **metadata_hash_dict(new_hash)}

    return dict()
