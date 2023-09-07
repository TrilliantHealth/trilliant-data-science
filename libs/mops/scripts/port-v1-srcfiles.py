#!/usr/bin/env python
import argparse
import json
from pathlib import Path

from thds.adls import AdlsFqn, resource


def port_srcfile_pointer_to_v2(filepath: Path, dry_run: bool = True):
    rewrite_as = None
    with open(filepath) as rf:
        d = json.loads(rf.read())
        if d.get("type") == "ADLS":
            # needs fixing
            rewrite_as = resource.of(AdlsFqn.of(d["sa"], d["container"], d["key"]), md5b64=d["md5b64"])

    if rewrite_as:
        if dry_run:
            print(f"Would rewrite {filepath.name} as {rewrite_as}")
        else:
            resource.to_path(filepath, rewrite_as)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filepaths", type=Path, nargs="+")
    parser.add_argument("--write", action="store_true", help="If not write, then do a dry run.")
    args = parser.parse_args()

    for filep in args.filepaths:
        port_srcfile_pointer_to_v2(filep, dry_run=not args.write)


if __name__ == "__main__":
    main()
