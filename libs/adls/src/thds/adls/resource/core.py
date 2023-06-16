import json
import typing as ty

from thds.core import log

from ..fqn import AdlsFqn
from ..md5 import check_reasonable_md5b64

logger = log.getLogger(__name__)


class AdlsHashedResource(ty.NamedTuple):
    fqn: AdlsFqn
    md5b64: str

    @property
    def serialized(self) -> str:
        return serialize(self)

    @staticmethod
    def of(fqn_or_uri: ty.Union[AdlsFqn, str], md5b64: str) -> "AdlsHashedResource":
        assert md5b64, "md5b64 must be non-empty"
        fqn = AdlsFqn.parse(fqn_or_uri) if isinstance(fqn_or_uri, str) else fqn_or_uri
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
