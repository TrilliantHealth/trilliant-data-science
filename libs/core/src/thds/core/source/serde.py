# this should later get promoted somewhere, probably
import json
import typing as ty
from pathlib import Path

from thds.core import files, hashing, log, types

from . import _construct
from .src import Source

_SHA256_B64 = "sha256b64"
_MD5_B64 = "md5b64"
MD5 = "md5"

logger = log.getLogger(__name__)


def _from_b64(d: dict) -> ty.Optional[hashing.Hash]:
    for key in d.keys():
        if key.endswith("b64"):
            algo = key[:-3]
            return hashing.Hash(algo=algo, bytes=hashing.db64(d[key]))
    return None


HashParser = ty.Callable[[dict], ty.Optional[hashing.Hash]]
_BASE_PARSERS = (_from_b64,)


def base_parsers() -> ty.Tuple[HashParser, ...]:
    return _BASE_PARSERS


def from_json(json_source: str, hash_parsers: ty.Collection[HashParser] = base_parsers()) -> Source:
    d = json.loads(json_source)
    return _construct.from_uri(
        uri=d["uri"],
        hash=next(filter(None, (p(d) for p in hash_parsers)), None),
    )


def _very_generic_b64_hash_serializer(hash: hashing.Hash) -> dict:
    return {hash.algo + "b64": hashing.b64(hash.bytes)}


HashSerializer = ty.Callable[[hashing.Hash], ty.Optional[dict]]
_BASE_HASH_SERIALIZERS: ty.Tuple[HashSerializer, ...] = (_very_generic_b64_hash_serializer,)


def base_hash_serializers() -> ty.Tuple[HashSerializer, ...]:
    return _BASE_HASH_SERIALIZERS


def to_json(
    source: Source, hash_serializers: ty.Collection[HashSerializer] = base_hash_serializers()
) -> str:
    hash_dict = (
        next(filter(None, (ser(source.hash) for ser in hash_serializers if source.hash)), None)
    ) or dict()
    return json.dumps(dict(uri=source.uri, **hash_dict))


def from_unknown_user_path(path: types.StrOrPath, desired_uri: str) -> Source:
    """Sometimes you may want to load a Source directly from a Path provided by a user.

    It _might_ represent something loadable as a from_json Source, but it might just be a
    raw file that needs to be loaded with from_file!

    This is a _reasonable_ (but not guaranteed!) way of trying to ascertain which one it
    is, and specifying where it should live 'remotely' if such a thing becomes
    necessary.

    Your application might need to implement something more robust if the
    actual underlying data is likely to be a JSON blob containing the key `uri`, for
    instance.
    """
    with open(path) as readable:
        try:
            return from_json(readable.read(4096))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return _construct.from_file(path, uri=desired_uri)


def write_to_json_file(source: Source, local_file: Path) -> bool:
    """Write the canonical JSON serialization of the Source to a file."""
    local_file.parent.mkdir(parents=True, exist_ok=True)
    previous_source = local_file.read_text() if local_file.exists() else None
    new_source = to_json(source) + "\n"
    if new_source != previous_source:
        with files.atomic_text_writer(local_file) as f:
            logger.info(f"Writing {source} to {local_file}")
            f.write(new_source)
            return True
    return False
