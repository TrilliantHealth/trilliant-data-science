"""Upload a file to the location under a given storage root where a
pathlib.Path would be put by the MemoizingPicklingFunctionRunner.
"""

import argparse
from pathlib import Path

from thds.adls.defaults import mops_root

from ..._utils.once import Once
from ..core import uris
from ..core.serialize_paths import CoordinatingPathSerializer, human_sha256b64_file_at_paths
from ..pickling import sha256_b64


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("file", help="Must be an actual file")
    parser.add_argument(
        "--upload-root-uri",
        "-u",
        help=f"Actually upload, using this URI as storage root. Example: {mops_root()}",
    )

    args = parser.parse_args()

    the_path = Path(args.file)
    human_hash = human_sha256b64_file_at_paths(the_path)

    print(human_hash)

    if args.upload_root_uri:
        storage_root = args.upload_root_uri.rstrip("/") + "/"
        with uris.ACTIVE_STORAGE_ROOT.set(storage_root):
            CoordinatingPathSerializer(sha256_b64.Sha256B64PathStream(), Once())(the_path)


if __name__ == "__main__":
    main()
