import json
import typing as ty

from thds.core import hashing, log, source
from thds.core.hashing import b64

from ..errors import blob_not_found_translation
from ..fqn import AdlsFqn
from ..global_client import get_global_fs_client
from ..md5 import check_reasonable_md5b64

logger = log.getLogger(__name__)


class AdlsHashedResource(ty.NamedTuple):
    """See the containing package for documentation on how to use this and its motivation."""

    fqn: AdlsFqn
    md5b64: str

    @property
    def serialized(self) -> str:
        return serialize(self)

    @staticmethod
    def of(fqn_or_uri: ty.Union[AdlsFqn, str], md5b64: str) -> "AdlsHashedResource":
        return of(fqn_or_uri, md5b64)

    @staticmethod
    def parse(serialized_dict: str) -> "AdlsHashedResource":
        return parse(serialized_dict)


def of(fqn_or_uri: ty.Union[AdlsFqn, str], md5b64: str) -> AdlsHashedResource:
    assert md5b64, "md5b64 must be non-empty"
    fqn = AdlsFqn.parse(fqn_or_uri) if isinstance(fqn_or_uri, str) else fqn_or_uri
    return AdlsHashedResource(fqn, md5b64)


def from_source(source: source.Source) -> AdlsHashedResource:
    assert source.hash, "Source must have a hash"
    assert source.hash.algo == "md5", f"Source Hash type must be MD5! Got: {source.hash.algo}"
    return of(source.uri, hashing.b64(source.hash.bytes))


def to_source(resource: AdlsHashedResource) -> source.Source:
    return source.from_uri(
        str(resource.fqn),
        hash=source.Hash("md5", hashing.db64(resource.md5b64)),
    )


def serialize(resource: AdlsHashedResource) -> str:
    d = resource._asdict()
    # we use uri instead of fqn in order to make these a more generic format
    return json.dumps(dict(uri=str(d["fqn"]), md5b64=d["md5b64"]))


def parse(serialized_dict: str) -> AdlsHashedResource:
    actual_dict = json.loads(serialized_dict)
    # accept either uri or fqn
    uri = actual_dict["uri"] if "uri" in actual_dict else actual_dict["fqn"]
    md5b64 = actual_dict["md5b64"]
    check_reasonable_md5b64(md5b64)
    return AdlsHashedResource.of(AdlsFqn.parse(uri), md5b64)


def get(fqn_or_uri: ty.Union[AdlsFqn, str]) -> AdlsHashedResource:
    """Creates an AdlsHashedResource from a remote-only file.

    The file _must_ have a pre-existing Content MD5!
    """
    fqn = AdlsFqn.parse(fqn_or_uri) if isinstance(fqn_or_uri, str) else fqn_or_uri
    with blob_not_found_translation(fqn):
        props = (
            get_global_fs_client(fqn.sa, fqn.container).get_file_client(fqn.path).get_file_properties()
        )
        assert props.content_settings.content_md5, "ADLS file has empty Content-MD5!"
        return AdlsHashedResource.of(fqn, b64(props.content_settings.content_md5))
