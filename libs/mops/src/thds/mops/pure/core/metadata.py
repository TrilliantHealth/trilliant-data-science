"""This is where we put implementation details having to do with the new metadata system in
v3 of mops.

Metadata is anything that is not critical to the core operation of mops but is useful for
debugging, monitoring, or other purposes.
"""

import argparse
import getpass
import os
import typing as ty
from dataclasses import dataclass
from datetime import datetime

from thds.core import calgitver, config, hostname

try:
    _CALGITVER = calgitver.calgitver()
except calgitver.git.NO_GIT:
    _CALGITVER = ""


INVOKER_CODE_VERSION = config.item("mops.metadata.local.invoker_code_version", _CALGITVER)
INVOKED_BY = config.item("mops.metadata.local.invoked_by", "")
REMOTE_CODE_VERSION = config.item("mops.metadata.remote.code_version", "")
# set the remote code version inside your docker image or other environment.


def get_invoker_code_version() -> str:
    return INVOKER_CODE_VERSION() or "unknown"


def get_invoked_by() -> str:
    return INVOKED_BY() or f"{getpass.getuser()}@{hostname.friendly()}"


@dataclass
class InvocationMetadata:
    """Metadata values may not contain spaces."""

    invoked_at: datetime
    invoked_by: str  # a more semantic identifier of 'who' called the function. This should be
    # passed recursively to other invocations.
    invoker_code_version: str
    # ^ Collectively: the 'ABC's of the invocation metadata.

    invoker_uuid: str  # the writer_uuid from the lock

    pipeline_id: str
    # technically not _just_ metadata, because it is used directly in
    # memoization. but this is a more convenient way to pass alongside
    # everything else that is used for debugging and monitoring.

    @staticmethod
    def new(pipeline_id: str, invoked_at: datetime, invoker_uuid: str) -> "InvocationMetadata":
        return InvocationMetadata(
            pipeline_id=pipeline_id,
            invoker_code_version=get_invoker_code_version(),
            invoker_uuid=invoker_uuid,
            invoked_at=invoked_at,
            invoked_by=get_invoked_by(),
        )


def get_remote_code_version(invoker_code_version: str) -> str:
    return (
        REMOTE_CODE_VERSION()
        or os.getenv("CALGITVER")
        or os.getenv("THDS_APP_VERSION")
        # these env var fallbacks are specifically for THDS internal use.
        # Control is exposed via the official config item.
        or invoker_code_version  # in a local-run context, use whatever was set explicitly, if anything.
    )


@dataclass
class ResultMetadata(InvocationMetadata):
    remote_code_version: str
    remote_started_at: datetime
    remote_ended_at: datetime
    # the below are redundant but useful to have precomputed:
    remote_wall_minutes: float  # between remote_started_at and remote_ended_at
    result_wall_minutes: float  # between remote_ended_at and invoked_at
    # we're using minutes because it's a more human-friendly unit of time,
    # and if you want the raw seconds you can always compute it from the original datetimes.

    @staticmethod
    def from_invocation(
        invocation_metadata: InvocationMetadata, started_at: datetime, ended_at: datetime
    ) -> "ResultMetadata":
        return ResultMetadata(
            **vars(invocation_metadata),
            remote_code_version=get_remote_code_version(invocation_metadata.invoker_code_version),
            remote_started_at=started_at,
            remote_ended_at=ended_at,
            remote_wall_minutes=(ended_at - started_at).total_seconds() / 60,
            result_wall_minutes=(ended_at - invocation_metadata.invoked_at).total_seconds() / 60,
        )


def invocation_metadata_parser(
    parser: ty.Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:
    parser = parser or argparse.ArgumentParser()
    assert parser
    parser.add_argument(
        "--invoked-by",
        help="Who invoked this function. Will be used recursively (for nested functions).",
        required=True,
    )
    parser.add_argument(
        "--invoker-code-version",
        help="The version of the code that is running. Usually a CalGitVer, but can be any non-empty string.",
        required=True,
    )
    parser.add_argument(
        "--invoked-at",
        help="The time at which this function was invoked. Should be an ISO8601 timestamp.",
        type=datetime.fromisoformat,
        required=True,
    )
    parser.add_argument(
        "--invoker-uuid",
        help="The UUID of the invoker. This is generally the writer UUID from the lock.",
    )
    parser.add_argument("--pipeline-id", required=True)
    return parser


def result_metadata_parser() -> argparse.ArgumentParser:
    parser = invocation_metadata_parser()
    parser.add_argument(
        "--remote-code-version",
        help="The version of the code that ran remotely. Usually a CalGitVer, but can be any non-empty string.",
    )
    parser.add_argument(
        "--remote-started-at",
        help="The time at which this function started. Should be an ISO8601 timestamp.",
        type=datetime.fromisoformat,
        required=True,
    )
    parser.add_argument(
        "--remote-ended-at",
        help="The time at which this function ended. Should be an ISO8601 timestamp.",
        type=datetime.fromisoformat,
        required=True,
    )
    parser.add_argument(
        "--remote-wall-minutes",
        help="The computed wall time in minutes between the remote start and end times.",
        type=float,
    )
    parser.add_argument(
        "--result-wall-minutes",
        help="The computed wall time in minutes between the remote end and the invocation time.",
        type=float,
    )
    return parser


def parse_invocation_metadata_args(args: ty.Sequence[str]) -> InvocationMetadata:
    """Parse metadata args from the command line.

    Metadata args are of the form --key-name=value.
    """
    metadata, _ = invocation_metadata_parser().parse_known_args(args)
    return InvocationMetadata(**vars(metadata))


def parse_result_metadata(metadata_keyvals: ty.Sequence[str]) -> ResultMetadata:
    """Parse metadata values from a result list.

    Metadata args are of the form key=value, and are separated by commas.

    Usually you'll be splitting an initial text string on newline, but we don't do that for you here.
    """

    def to_arg(kv: str) -> str:
        try:
            key, value = kv.split("=", 1)
            return f"--{key.replace('_', '-')}={value}"
        except ValueError:
            raise ValueError(f"Unable to parse metadata key-value pair {kv}. Must be key=value.")

    metadata = result_metadata_parser().parse_args([to_arg(kv) for kv in metadata_keyvals if kv])
    return ResultMetadata(**vars(metadata))


def _format_metadata(
    metadata: ty.Union[InvocationMetadata, ResultMetadata], prefix: str
) -> ty.List[str]:
    """Format metadata args for the command line OR for the header in a result payload.

    Metadata args are of the form key=value, and are separated by commas.
    """

    def to_str(value: ty.Any) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def nospaces_to_str(value: ty.Any) -> str:
        s = to_str(value)
        if " " in s:
            raise ValueError(f"Metadata value {s} contains a space. This is illegal")
        return s

    return [
        f"{prefix}{k.replace('_', '-')}={nospaces_to_str(v)}"
        for k, v in vars(metadata).items()
        if v is not None and v != ""
    ]


def format_invocation_cli_args(metadata: InvocationMetadata) -> ty.List[str]:
    return _format_metadata(metadata, prefix="--")


def format_result_header(metadata: ResultMetadata) -> str:
    """Includes separating newlines and a trailing newline."""
    return "\n".join(_format_metadata(metadata, prefix="")) + "\n"


def format_end_of_run_times(start_timestamp: float, maybe_metadata_args: ty.Sequence[str]) -> str:
    import time

    try:
        meta = parse_invocation_metadata_args(maybe_metadata_args)
        wait_time = start_timestamp - meta.invoked_at.timestamp()
        total_time = time.time() - meta.invoked_at.timestamp()
        return f" (waited {wait_time/60:.2f} minutes, total time {total_time/60:.2f} minutes) - version: {meta.invoker_code_version}"
    except Exception:
        return ""
