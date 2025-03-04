"""Uses rsync to copy files to/from a Kubernetes pod.

The remote pod must have rsync installed.

CLI wrapper with help text for the krsync.sh script, which is usable on its own.
Thank you, Karl Bunch, who provided the world with this elegant implementation.
https://serverfault.com/questions/741670/rsync-files-to-a-kubernetes-pod?newreg=22b5f958cdce4e6a9a1a7ce0fc88b546

When addressing the remote, you must specify a pod name, and
optionally a namespace preceded by '@', and then a colon, followed by
the path on the remote.

Examples:

krsync ~/my/local.txt pod1234:/root/local_2.txt
krsync ~/my/local pod1234:~/local_dir -rav  # recursively copies entire directory
krsync pod1234@my-namespace:/root/my.parquet your.parquet
krsync prod-udla-0@unified-directory:/var/data/labels.db ./labels.db --container prod-udla-db
"""

import argparse
import importlib
import os
import subprocess
import sys

with importlib.resources.path(__package__, "krsync.sh") as p:
    krsync = str(p.resolve())


def main() -> int:
    remote_path = "pod-name[@namespace]:/remote/path"
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("src", help=f"Either a local path or {remote_path}")
    parser.add_argument("dest", help=f"Either a local path or {remote_path}")
    parser.add_argument(
        "--container",
        "-c",
        help="Container name - if not provided, will use the default container",
        default="",
    )
    args, rsync_args = parser.parse_known_args()
    return subprocess.run(
        ["/bin/bash", krsync, args.src, args.dest, *rsync_args],
        env=dict(os.environ, KRSYNC_CONTAINER=args.container or ""),
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
