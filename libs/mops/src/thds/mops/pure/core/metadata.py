"""This is where we put implementation details having to do with the new metadata system in
v3 of mops.

Metadata is anything that is not critical to the core operation of mops but is useful for
debugging, monitoring, or other purposes.
"""

import argparse
import getpass
import importlib
import logging
import os
import typing as ty
from dataclasses import dataclass, field
from datetime import datetime

from thds.core import calgitver, config, hostname, log

_logger = logging.getLogger(__name__)

INVOKER_CODE_VERSION = config.item("mops.metadata.local.invoker_code_version", "")
INVOKED_BY = config.item("mops.metadata.local.invoked_by", "")
REMOTE_CODE_VERSION = config.item("mops.metadata.remote.code_version", "")
EXTRA_METADATA_GENERATOR = config.item("mops.metadata.extra_generator", default="")
# Dotted import path to a callable that generates extra metadata fields.
# The callable signature is: (ResultMetadata) -> dict[str, str]
# Return key-value pairs to include in the metadata file under "=== Extra Metadata ===".
# set the remote code version inside your docker image or other environment.

MetadataGenerator = ty.Callable[["ResultMetadata"], ty.Dict[str, str]]


_generator_cache: ty.Dict[str, ty.Optional[MetadataGenerator]] = {}


def load_metadata_generator() -> ty.Optional[MetadataGenerator]:
    """Load the configured extra metadata generator, if any.

    Caches per import path so import errors warn once per process per config value.
    """
    import_path = EXTRA_METADATA_GENERATOR()
    if not import_path:
        return None

    if import_path in _generator_cache:
        return _generator_cache[import_path]

    try:
        module_path, func_name = import_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
        _generator_cache[import_path] = ty.cast(MetadataGenerator, func)
    except (ValueError, ImportError, AttributeError) as e:
        _logger.warning(f"Failed to load extra metadata generator '{import_path}': {e}")
        _generator_cache[import_path] = None

    return _generator_cache[import_path]


def log_context_metadata() -> ty.Dict[str, str]:
    """Whatever tagged this invocation's logs, to record alongside its result.

    A launcher that runs many invocations in one process tags each one's lines with what
    it is; recording those same fields is what lets a reader get from a result back to the
    lines that produced it. `mops` does not interpret them - it sets none of them, and
    which keys mean what is the launcher's business.

    Call this before entering any scope of your own, so the snapshot is the launcher's
    context and not a description of what `mops` was doing when it looked. The whole
    context, not just `log.env`: a launcher that runs items on its own threads has no
    child to hand an environment to, so only `logger_context` carries its tags.

    Values with a space or a newline are dropped: the metadata file is `key=value` lines
    parsed by splitting, and a log context is not ours to constrain.
    """
    return {
        key: str(value)
        for key, value in log.logger_context_values().items()
        if str(value) and not any(c in str(value) for c in " \n\r")
    }


_EXTRA_SECTION = "=== Extra Metadata ==="
LOG_CONTEXT_SECTION = "=== Log Context ==="
# Written after every section a mops before 3.34.20260923 reads: those readers stop at the
# first section they do not know, and they parse the ones they do as command-line flags, so
# a context key such as `remote` would be an ambiguous abbreviation that exits the process.


def _format_section(header: str, keyvals: ty.Mapping[str, str]) -> str:
    if not keyvals:
        return ""

    return "\n".join(["", header, *(f"{k}={v}" for k, v in sorted(keyvals.items()))]) + "\n"


def format_extra_metadata(extra: ty.Mapping[str, str]) -> str:
    return _format_section(_EXTRA_SECTION, extra)


def format_log_context_metadata(log_context: ty.Mapping[str, str]) -> str:
    return _format_section(LOG_CONTEXT_SECTION, log_context)


def get_invoker_code_version() -> str:
    if v := INVOKER_CODE_VERSION():
        return v

    try:
        return calgitver.calgitver() or "unknown"
    except calgitver.git.NO_GIT:
        return "unknown"


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

    invoker_uuid: str  # the writer_uuid from the lease

    pipeline_id: str
    # technically not _just_ metadata, because it is used directly in
    # memoization. but this is a more convenient way to pass alongside
    # everything else that is used for debugging and monitoring.

    console_run_name: str
    # names the orchestrator run for observability; empty when the console is disabled.
    # No default: ResultMetadata extends this, and a defaulted field here would force
    # defaults onto every field it adds. `new()` and the arg parser both supply "".
    # Older remotes parse with parse_known_args and simply ignore the flag.

    @staticmethod
    def new(
        pipeline_id: str, invoked_at: datetime, invoker_uuid: str, console_run_name: str = ""
    ) -> "InvocationMetadata":
        return InvocationMetadata(
            pipeline_id=pipeline_id,
            invoker_code_version=get_invoker_code_version(),
            invoker_uuid=invoker_uuid,
            invoked_at=invoked_at,
            invoked_by=get_invoked_by(),
            console_run_name=console_run_name,
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
    run_id: str = ""
    # unique identifier for this execution, used in output paths and metadata filenames.
    # format: YYMMDDHHmm-TwoWords (e.g., 2601271523-SkirtBus)
    extra: ty.Dict[str, str] = field(default_factory=dict)
    # extra metadata from generators (e.g., grafana_logs, k8s_pod_name)

    @staticmethod
    def from_invocation(
        invocation_metadata: InvocationMetadata,
        started_at: datetime,
        ended_at: datetime,
        run_id: str = "",
    ) -> "ResultMetadata":
        return ResultMetadata(
            **vars(invocation_metadata),
            remote_code_version=get_remote_code_version(invocation_metadata.invoker_code_version),
            remote_started_at=started_at,
            remote_ended_at=ended_at,
            remote_wall_minutes=(ended_at - started_at).total_seconds() / 60,
            result_wall_minutes=(ended_at - invocation_metadata.invoked_at).total_seconds() / 60,
            run_id=run_id,
        )


def invocation_metadata_parser(
    parser: ty.Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:
    parser = parser or argparse.ArgumentParser(allow_abbrev=False)
    assert parser
    # no abbreviation: extra metadata reaches this parser as `--<key>=<value>` too, and a key
    # that happens to prefix a real flag (`remote` against `--remote-code-version`) is an
    # ambiguous option rather than the unrecognized one it should be. Metadata is always
    # written with full flag names, so nothing legitimate relies on abbreviating.
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
        help="The UUID of the invoker. This is generally the writer UUID from the lease.",
    )
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--console-run-name", default="")
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
    parser.add_argument(
        "--run-id",
        help="Unique identifier for this execution (format: YYMMDDHHmm-TwoWords).",
        default="",
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

    Metadata args are of the form key=value, and are separated by newlines.
    Continues through the extra-metadata and log-context sections but stops at
    any other === section (forward-compatible with future sections).

    Extra key=value pairs not recognized by the parser, and the log context, are
    captured in the `extra` field, making them available to tools like mops-inspect.
    The log context is never parsed as flags, so its keys cannot set a field.
    """
    filtered_lines: ty.List[str] = []
    log_context: ty.Dict[str, str] = {}
    section = ""
    for line in metadata_keyvals:
        if line.startswith("==="):
            if line not in (_EXTRA_SECTION, LOG_CONTEXT_SECTION):
                break

            section = line
            continue

        if not line:
            continue

        if section == LOG_CONTEXT_SECTION:
            key, sep, value = line.partition("=")
            if sep:
                log_context[key] = value
        else:
            filtered_lines.append(line)

    def to_arg(kv: str) -> ty.Optional[str]:
        try:
            key, value = kv.split("=", 1)
            return f"--{key.replace('_', '-')}={value}"
        except ValueError:
            return None

    args = [a for a in (to_arg(kv) for kv in filtered_lines) if a is not None]
    metadata, unknown = result_metadata_parser().parse_known_args(args)

    # Capture extra key=value pairs that weren't recognized by the parser.
    # Skip 'extra' itself - old files may have written extra={} which we don't want to nest.
    extra: ty.Dict[str, str] = {}
    for arg in unknown:
        if arg.startswith("--") and "=" in arg:
            key, value = arg[2:].split("=", 1)
            key = key.replace("-", "_")
            if key != "extra":
                extra[key] = value

    return ResultMetadata(**vars(metadata), extra={**log_context, **extra})


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
        # skip 'extra' - it's a dict handled separately by format_extra_metadata
        if v is not None and v != "" and k != "extra"
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
        return f" (waited {wait_time / 60:.2f} minutes, total time {total_time / 60:.2f} minutes) - version: {meta.invoker_code_version}"
    except Exception:
        return ""
