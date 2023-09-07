import os

from thds.adls import download, ro_cache
from thds.adls.global_client import get_global_client
from thds.core.types import StrOrPath

from ....srcdest.remote_file import Serialized
from ....srcdest.up_down import DOWNLOADERS
from .parse_serialized import resource_from_serialized

srcfile_cache = ro_cache.global_cache()
# used in testing


def download_serialized(serialized: Serialized, local_dest: StrOrPath):
    ahr = resource_from_serialized(serialized)
    # This gives us a machine-global cache to use, and also gives us maximum possible
    # verification if we have the md5 in the serialized pointer.
    #
    # The types of bugs this would catch would be if a file uploaded
    # to ADLS got modified after its initial upload. DestFiles and
    # SrcFiles are not designed to be mutable - they should be
    # write-once, as per the documentation.
    fqn = ahr.fqn
    download.download_or_use_verified(
        get_global_client(fqn.sa, fqn.container),
        fqn.path,
        os.fspath(local_dest),
        md5b64=ahr.md5b64,
        cache=srcfile_cache,
    )


# never, ever change this key, since it is serialized along with the URI in SrcFiles
ADLS_DOWNLOAD_V1 = "adls_serialized_v1"
DOWNLOADERS[ADLS_DOWNLOAD_V1] = download_serialized
