import os
import typing as ty

from thds.core.hashing import b64

from ..global_client import get_global_client
from .core import AdlsHashedResource, parse, serialize

_AZURE_PLACEHOLDER_SIZE_LIMIT = 4096
# it is assumed that no placeholder will ever need to be larger than 4 KB.


def resource_from_path(path: ty.Union[str, os.PathLike]) -> AdlsHashedResource:
    """Raises if the path does not represent a serialized AdlsHashedResource."""
    with open(path) as maybe_json_file:
        json_str = maybe_json_file.read(_AZURE_PLACEHOLDER_SIZE_LIMIT)
        return parse(json_str)


def resource_to_path(path: ty.Union[str, os.PathLike], resource: AdlsHashedResource) -> None:
    with open(path, "w") as json_file:
        json_file.write(serialize(resource) + "\n")


def validate_resource(srcfile: ty.Union[str, os.PathLike]) -> AdlsHashedResource:
    res = resource_from_path(srcfile)
    fqn, md5b64 = res
    props = get_global_client(fqn.sa, fqn.container).get_file_client(fqn.path).get_file_properties()
    md5 = props.content_settings.content_md5
    assert md5, f"{fqn} was incorrectly uploaded to ADLS without an MD5 embedded."
    assert md5b64 == b64(md5), f"You probably need to update the MD5 in {srcfile}"
    return res
