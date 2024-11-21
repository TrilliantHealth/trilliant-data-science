import datetime as dt
import json
import os
import typing as ty
import uuid
from pathlib import Path

from thds.core import config, log
from thds.mops.pure.core.memo import function_memospace
from thds.mops.pure.core.types import T

from ...core import metadata

MOPS_SUMMARY_DIR = config.item("thds.mops.summary_dir", default=Path(".mops"), parse=Path)
RUN_NAME_ENV_VAR: ty.Final = "__SECRET_THDS_MOPS_RUN_NAME"

InvocationType = ty.Literal["memoized", "invoked", "awaited"]

logger = log.getLogger(__name__)


class LogEntryV1(ty.TypedDict):
    function_name: str
    memo_uri: str
    timestamp: str  # more or less "when did this complete?"
    status: InvocationType  # old name that we're retaining for compatibility


class LogEntry(LogEntryV1, total=False):
    runner_prefix: str  # includes env and any prefixes like mops2-mpf
    pipeline_id: str
    function_logic_key: str
    was_error: bool

    total_runtime_minutes: float
    remote_runtime_minutes: float
    invoked_by: str
    invoker_code_version: str
    remote_code_version: str


if not os.getenv(RUN_NAME_ENV_VAR):
    os.environ[RUN_NAME_ENV_VAR] = f"{dt.datetime.utcnow().isoformat()}-{os.getpid()}"


def create_mops_run_directory() -> Path:
    assert RUN_NAME_ENV_VAR in os.environ, f"{RUN_NAME_ENV_VAR} is not set"

    # Define the root directory for mops logs
    mops_root = MOPS_SUMMARY_DIR()
    # Use run name if set, otherwise fallback to orchestrator datetime
    run_name = os.environ[RUN_NAME_ENV_VAR]
    # Create a subdirectory named with the orchestrator datetime and run identifier
    run_directory = mops_root / run_name
    try:
        run_directory.mkdir(parents=True, exist_ok=True)
    except Exception:
        if mops_root.exists() and not mops_root.is_dir():
            # this is going to cause errors later on!
            logger.error(
                f"mops summary directory must be a directory: '{mops_root}'"
                " Please delete this file and allow mops to recreate it!"
            )
        else:
            raise

    return run_directory


def _generate_log_filename(run_directory: Path) -> Path:
    """Generate a log filename using the current timestamp and a short UUID, ensuring uniqueness"""
    timestamp = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    short_uuid = str(uuid.uuid4())[:8]
    filename = f"{timestamp}-{short_uuid}.json"
    return run_directory / filename


def log_function_execution(
    run_directory: ty.Optional[Path],
    func: ty.Callable[..., T],
    memo_uri: str,
    itype: InvocationType,
    metadata: ty.Optional[metadata.ResultMetadata] = None,
    runner_prefix: str = "",
    was_error: bool = False,
) -> None:
    if not run_directory:
        logger.debug("Not writing function summary for %s", memo_uri)
        return

    log_file = _generate_log_filename(run_directory)
    func_module = func.__module__
    func_name = func.__name__
    full_function_name = f"{func_module}:{func_name}"

    parts = function_memospace.parse_memo_uri(memo_uri, runner_prefix)

    log_entry: LogEntry = {
        "function_name": full_function_name,
        "memo_uri": memo_uri,
        "runner_prefix": parts.runner_prefix,
        "pipeline_id": parts.pipeline_id,
        "function_logic_key": parts.function_logic_key,
        "timestamp": dt.datetime.utcnow().isoformat(),
        "status": itype,
        "was_error": was_error,
    }
    if metadata:
        log_entry["total_runtime_minutes"] = metadata.result_wall_minutes
        log_entry["remote_runtime_minutes"] = metadata.remote_wall_minutes
        log_entry["invoked_by"] = metadata.invoked_by
        log_entry["invoker_code_version"] = metadata.invoker_code_version
        log_entry["remote_code_version"] = metadata.remote_code_version
        # we don't bother with invoked_at or remote_started_at because they can be
        # inferred from the timestamp and the wall times
    try:
        assert not log_file.exists(), f"Log file '{log_file}' should not already exist"
        with log_file.open("w") as f:
            json.dump(log_entry, f, indent=2)
    except Exception:
        logger.exception(f"Unable to write mops function invocation log file at '{log_file}'")
