# this should later get promoted somewhere, probably
import json
import typing as ty
from functools import partial

from thds.core import hashing, source

_SHA256_B64 = "sha256b64"
_MD5_B64 = "md5b64"


def _from_sha256b64(d: dict) -> ty.Optional[hashing.Hash]:
    if "sha256b64" in d:
        return hashing.Hash(algo="sha256", bytes=hashing.db64(d[_SHA256_B64]))
    return None


def _from_md5b64(d: dict) -> ty.Optional[hashing.Hash]:
    if "md5b64" in d:
        return hashing.Hash(algo="md5", bytes=hashing.db64(d[_MD5_B64]))
    return None


HashParser = ty.Callable[[dict], ty.Optional[hashing.Hash]]
_BASE_PARSERS = (_from_sha256b64, _from_md5b64)


def base_parsers() -> ty.Tuple[HashParser, ...]:
    return _BASE_PARSERS


def from_json(
    json_source: str, hash_parsers: ty.Collection[HashParser] = base_parsers()
) -> source.Source:
    d = json.loads(json_source)
    return source.from_uri(
        uri=d["uri"],
        hash=next(filter(None, (p(d) for p in hash_parsers)), None),
    )


def _generic_hash_serializer(
    algo: str, stringify_hash: ty.Callable[[bytes], str], keyname: str, hash: hashing.Hash
) -> ty.Optional[dict]:
    if hash.algo == algo:
        return {keyname: stringify_hash(hash.bytes)}
    return None


_to_sha256b64 = partial(_generic_hash_serializer, "sha256", hashing.b64, _SHA256_B64)
_to_md5b64 = partial(_generic_hash_serializer, "md5", hashing.b64, _MD5_B64)

HashSerializer = ty.Callable[[hashing.Hash], ty.Optional[dict]]
_BASE_HASH_SERIALIZERS: ty.Tuple[HashSerializer, ...] = (_to_md5b64, _to_sha256b64)  # type: ignore


def base_hash_serializers() -> ty.Tuple[HashSerializer, ...]:
    return _BASE_HASH_SERIALIZERS


def to_json(
    source: source.Source, hash_serializers: ty.Collection[HashSerializer] = base_hash_serializers()
) -> str:
    hash_dict = (
        next(filter(None, (ser(source.hash) for ser in hash_serializers if source.hash)), None)
    ) or dict()
    return json.dumps(dict(uri=source.uri, **hash_dict))
