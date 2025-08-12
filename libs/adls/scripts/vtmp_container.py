"""
Our scratch vtmp container has a retention policy of 4 days since "Last Access".
The "Last Accessed Time" is an internal attribute that ADLS manages, and though
it would be useful to be able to examine its value, ADLS does not currently
provide a way for you to do that -- they keep it private.

If you know you have some blobs you want to hold onto beyond 4 days, this script
provides a way for you to do that.  Unfortunately, you will need to collect
those blobs' URIs first.
"""

import typing as ty

from thds import adls
from thds.core import log, parallel, scope, tmp
from thds.core.thunks import thunking

logger = log.getLogger(__name__)


# init: useful for testing


class InitArgs(ty.NamedTuple):
    n: int


@scope.bound
def _get_or_create_blob(n: int) -> adls.AdlsFqn:
    blob = adls.named_roots.require("vtmp") / f"test/last-access-time-modification-{n}.txt"

    gc = adls.global_client.get_global_fs_client(blob.sa, blob.container)
    if gc.get_file_client(blob.path).exists():
        logger.info(f"Blob already exists {blob}")
        return blob

    tmp_file = scope.enter(tmp.temppath_same_fs())
    tmp_file.write_text("adsf")
    adls.upload(blob, tmp_file)
    logger.info(f"Created {blob}")
    return blob


def _get_or_create_blobs(args: InitArgs) -> list[adls.AdlsFqn]:
    """If you're wanting to test this from scratch, this is an easy way to generate some test blobs."""
    logger.info(f"init: making sure {args.n} blobs exist")
    return [_get_or_create_blob(n) for n in range(args.n)]


# bump: the actually-useful feature of this script


class BumpArgs(ty.NamedTuple):
    uris: ty.Collection[str]


def _download_1_byte_of_blob(blob: adls.AdlsFqn) -> adls.AdlsFqn:
    gc = adls.global_client.adls_blob_container_client(blob.sa, blob.container)
    blob_client = gc.get_blob_client(blob.path)
    content_read = blob_client.download_blob(offset=0, length=1).readall()
    logger.info(f"Read 1 byte of blob {blob}: '{str(content_read, 'utf-8')}'")
    return blob


def _download_1_byte_of_blobs(args: BumpArgs) -> list[adls.AdlsFqn]:
    """downloading some part of a blob alters its elusive 'Last Accessed Time' property.  that's what we do here.
    if you want to hold onto a blob in vtmp beyond 4 days, give its uri to this command.
    https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-policy-structure#access-time-tracking
    """
    bump_thunks = iter(thunking(_download_1_byte_of_blob)(adls.parse_any(uri)) for uri in args.uris)
    return sorted(parallel.yield_results(bump_thunks))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.set_defaults(func=lambda _: parser.print_help())
    subparsers = parser.add_subparsers()

    parser_init = subparsers.add_parser("init", help=_get_or_create_blobs.__doc__)
    parser_init.add_argument(
        "-n", type=int, default=1, help="How many test-blobs do you want to create?"
    )
    parser_init.set_defaults(func=_get_or_create_blobs)

    parser_bump = subparsers.add_parser("bump", help=_download_1_byte_of_blobs.__doc__)
    parser_bump.add_argument(
        "uris", nargs="+", help="FQNs of blobs of which you wish to bump the LastAccessedTime"
    )
    parser_bump.set_defaults(func=_download_1_byte_of_blobs)

    args = parser.parse_args()

    result = args.func(args)
    print("\n".join(map(str, result)))
