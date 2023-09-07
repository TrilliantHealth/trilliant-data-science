"""Composes the remote_file interface with the _adls implementation."""
import json
import typing as ty
from functools import lru_cache

from thds.adls import AdlsFqn, resource
from thds.core.log import getLogger

from ....srcdest.remote_file import Serialized, StrOrPath

_AZURE_PLACEHOLDER_SIZE_LIMIT = 4096
# it is assumed that no placeholder will ever need to be larger than 4 KB.
logger = getLogger(__name__)


@lru_cache(maxsize=256)
def _try_parse_adls_path_repr(
    possible_json_adls_repr: str,
) -> ty.Optional[resource.AHR]:
    try:
        ahr_dict = json.loads(possible_json_adls_repr)
        return resource.AHR(
            fqn=AdlsFqn.parse(ahr_dict.pop("uri")), md5b64=ahr_dict.pop("md5b64", "") or ""
        )
    except (json.JSONDecodeError, TypeError, KeyError):
        return None


def resource_from_serialized(serialized: Serialized) -> resource.AHR:
    """Please note that this Resource may be technically invalid, missing an md5b64.

    It is only suitable for mops-internal use.
    """
    if not serialized:
        raise ValueError("Completely empty serialization makes no sense.")
    ahr = _try_parse_adls_path_repr(serialized)
    if not ahr:
        raise TypeError(f"Not an instance of ADLS Serialized: <<{serialized}>>")
    return ahr


def read_possible_serialized(local_src: StrOrPath) -> ty.Optional[resource.AHR]:
    """Open a known-real file and see if it is a Serialized remote pointer."""
    with open(local_src) as maybe_json_file:
        try:
            json_str = maybe_json_file.read(_AZURE_PLACEHOLDER_SIZE_LIMIT)
            return _try_parse_adls_path_repr(json_str)
        except UnicodeDecodeError:
            # TODO determine proper exception types to catch here
            pass
    return None
