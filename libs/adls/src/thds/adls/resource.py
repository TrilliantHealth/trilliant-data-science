import json
import os
import typing as ty

from thds.core.hashing import b64

from .fqn import AdlsFqn
from .global_client import get_global_client
from .md5 import check_reasonable_md5b64


class AdlsHashedResource(ty.NamedTuple):
    fqn: AdlsFqn
    md5b64: str

    @property
    def serialized(self) -> str:
        return serialize(self)

    @staticmethod
    def of(fqn: AdlsFqn, md5b64: str) -> "AdlsHashedResource":
        assert md5b64, "md5b64 must be non-empty"
        return AdlsHashedResource(fqn, md5b64)

    @staticmethod
    def parse(serialized_dict: str) -> "AdlsHashedResource":
        return parse(serialized_dict)


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


_AZURE_PLACEHOLDER_SIZE_LIMIT = 4096
# it is assumed that no placeholder will ever need to be larger than 4 KB.


def resource_from_path(path: ty.Union[str, os.PathLike]) -> AdlsHashedResource:
    """Raises if the path does not represent a serialized AdlsHashedResource."""
    with open(path) as maybe_json_file:
        json_str = maybe_json_file.read(_AZURE_PLACEHOLDER_SIZE_LIMIT)
        return parse(json_str)


def validate_resource(srcfile: ty.Union[str, os.PathLike]) -> AdlsHashedResource:
    res = resource_from_path(srcfile)
    fqn, md5b64 = res
    props = get_global_client(fqn.sa, fqn.container).get_file_client(fqn.path).get_file_properties()
    md5 = props.content_settings.content_md5
    assert md5, f"{fqn} was incorrectly uploaded to ADLS without an MD5 embedded."
    assert md5b64 == b64(md5), f"You probably need to update the MD5 in {srcfile}"
    return res
