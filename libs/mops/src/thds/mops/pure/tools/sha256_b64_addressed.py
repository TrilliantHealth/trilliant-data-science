"""Upload a file to the location under a given storage root where a
pathlib.Path would be put by the MemoizingPicklingFunctionRunner.
"""

import argparse
from pathlib import Path

from ..._utils.once import Once
from ..core import uris
from ..core.serialize_paths import CoordinatingPathSerializer, human_sha256b64_file_at_paths
from ..pickling import sha256_b64


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("file", help="Must be an actual file")
    parser.add_argument(
        "--upload-root-uri",
        "-u",
        help="Actually upload, using this URI as storage root. Example: adls://thdsscratch/tmp/",
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
