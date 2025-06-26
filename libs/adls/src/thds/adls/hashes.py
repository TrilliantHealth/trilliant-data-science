import contextlib
import os
import typing as ty
from functools import partial

from thds.core import hash_cache, hashing, log, types
from thds.core.hashing import Hash

from . import blake_hash, errors, file_properties
from .fqn import AdlsFqn


def make_hashes(*, md5b64: str = "", blake3_b64: str = "") -> tuple[hashing.Hash, ...]:
    hashes = list()
    if blake3_b64:
        hashes.append(hashing.Hash(algo="blake3", bytes=hashing.db64(blake3_b64)))
    if md5b64:
        hashes.append(hashing.Hash(algo="md5", bytes=hashing.db64(md5b64)))
    return tuple(hashes)


def hash_file(algo: str, path: ty.Union[str, os.PathLike]) -> hashing.Hash:
    if algo == "blake3":
        return blake_hash.blake3_file(path)
    return hash_cache.filehash(algo, path)


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
    return partial(_hash_path_if_exists, partial(hash_file, algo))


@contextlib.contextmanager
def verify_hashes_before_and_after_download(
    remote_hash: ty.Optional[Hash],
    expected_hash: ty.Optional[Hash],
    fqn: AdlsFqn,
    local_dest: types.StrOrPath,
) -> ty.Iterator[None]:
    # if expected_hash:
    #     check_reasonable_md5b64(expected_md5b64)
    # if remote_md5b64:
    #     check_reasonable_md5b64(remote_md5b64)
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

    with log.logger_context(hash_for="after-download"):
        local_hash = hash_file(expected_algo, local_dest)

    if remote_hash and remote_hash != local_hash:
        raise errors.HashMismatchError(
            f"The {local_hash.algo} of the downloaded file {local_dest} is {hashing.b64(local_hash.bytes)},"
            f" but the remote ({fqn}) says it should be {hashing.b64(remote_hash.bytes)}."
            f" This may indicate that ADLS has an erroneous MD5 for {fqn}."
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


def create_hash_metadata_if_missing(
    file_properties: ty.Optional[file_properties.FileProperties], new_hash: ty.Optional[Hash]
) -> dict:
    if not (file_properties and new_hash):
        # without file properties, we can't match the etag when we try to set this.
        return dict()

    existing_metadata = file_properties.metadata or dict()
    if "hash_blake3_b64" not in existing_metadata and new_hash.algo == "blake3":
        return {**existing_metadata, "hash_blake3_b64": hashing.b64(new_hash.bytes)}

    return dict()
