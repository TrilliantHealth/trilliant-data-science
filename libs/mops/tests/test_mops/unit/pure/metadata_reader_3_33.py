"""The result-metadata reader as mops 3.33 and earlier shipped it, frozen here so tests can
check that what mops writes today is still readable by processes that have not upgraded.

Copied from `thds/mops/pure/core/metadata.py` at the parent of the #5690 merge
(20260921, 3431fda7cd^1), with help strings dropped and the result returned as a dict
rather than a `ResultMetadata`. Do not update it to match the current reader: its whole
value is that it does not change. Note `argparse.ArgumentParser()` with abbreviation left
on, and that the `=== Extra Metadata ===` section is parsed as flags.
"""

import argparse
import typing as ty
from datetime import datetime


def _result_metadata_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invoked-by", required=True)
    parser.add_argument("--invoker-code-version", required=True)
    parser.add_argument("--invoked-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--invoker-uuid")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--console-run-name", default="")
    parser.add_argument("--remote-code-version")
    parser.add_argument("--remote-started-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--remote-ended-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--remote-wall-minutes", type=float)
    parser.add_argument("--result-wall-minutes", type=float)
    parser.add_argument("--run-id", default="")
    return parser


def parse_result_metadata(metadata_keyvals: ty.Sequence[str]) -> ty.Dict[str, ty.Any]:
    whitelisted_sections = {"=== Extra Metadata ==="}

    filtered_lines: ty.List[str] = []
    for line in metadata_keyvals:
        if line.startswith("===") and line not in whitelisted_sections:
            break

        if line in whitelisted_sections:
            continue

        if line:
            filtered_lines.append(line)

    def to_arg(kv: str) -> ty.Optional[str]:
        try:
            key, value = kv.split("=", 1)
            return f"--{key.replace('_', '-')}={value}"
        except ValueError:
            return None

    args = [a for a in (to_arg(kv) for kv in filtered_lines) if a is not None]
    metadata, unknown = _result_metadata_parser().parse_known_args(args)

    extra: ty.Dict[str, str] = {}
    for arg in unknown:
        if arg.startswith("--") and "=" in arg:
            key, value = arg[2:].split("=", 1)
            key = key.replace("-", "_")
            if key != "extra":
                extra[key] = value

    return {**vars(metadata), "extra": extra}
