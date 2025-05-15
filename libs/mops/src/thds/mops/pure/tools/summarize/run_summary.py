import datetime as dt
import json
import os
import pickle
import typing as ty
from pathlib import Path

from thds.core import config, log, pickle_visit, source
from thds.mops.pure.core.memo import function_memospace
from thds.mops.pure.core.metadata import get_invoked_by

from ...core import metadata

MOPS_SUMMARY_DIR = config.item("thds.mops.summary.dir", default=Path(".mops/summary"), parse=Path)
RUN_NAME = config.item(
    "thds.mops.summary.run_name",
    default=f"{dt.datetime.utcnow().isoformat()}-pid{os.getpid()}-{get_invoked_by()}",
)

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

    uris_in_args_kwargs: ty.List[str]
    uris_in_rvalue: ty.List[str]


def create_mops_run_directory() -> Path:
    # Define the root directory for mops logs
    mops_root = MOPS_SUMMARY_DIR()
    # Use run name if set, otherwise fallback to orchestrator datetime
    run_name = RUN_NAME()
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


def _generate_log_filename(
    run_directory: Path, invoked_at: dt.datetime, name: str, args_hash: str
) -> Path:
    """Generate a log filename using an invoked_at timestamp, the function name, and part
    of the args hash, ensuring uniqueness.
    """
    timestamp = invoked_at.strftime("%Y%m%d%H%M%S")
    filename = f"{timestamp}-{args_hash[:20]}-{name}.json"
    return run_directory / filename


def _extract_source_uris(result: ty.Any) -> ty.Set[str]:
    uris: ty.Set[str] = set()

    def extract_uri(unknown: ty.Any) -> None:
        if hasattr(unknown, "stored_uri") and isinstance(unknown.stored_uri, str):
            uris.add(unknown.stored_uri)
        if isinstance(unknown, source.Source):
            uris.add(unknown.uri)
        if hasattr(unknown, "sa") and hasattr(unknown, "container") and hasattr(unknown, "path"):
            uris.add(str(unknown))
        if isinstance(unknown, str) and (unknown.startswith("adls://") or unknown.startswith("file://")):
            uris.add(unknown)
        if hasattr(unknown, "material"):
            material = unknown.material
            if callable(material):
                mat = material()
                for uri in _extract_source_uris(mat):
                    uris.add(uri)

    try:
        pickle_visit.recursive_visit(extract_uri, result)
    except pickle.PicklingError:
        pass
    except Exception as exc:
        logger.warning(f'Unexpected error trying to extract URIs from "%s"; {exc}', result)

    return uris


def log_function_execution(
    run_directory: ty.Optional[Path],
    memo_uri: str,
    itype: InvocationType,
    metadata: ty.Optional[metadata.ResultMetadata] = None,
    runner_prefix: str = "",
    was_error: bool = False,
    return_value: ty.Any = None,
    args_kwargs: ty.Any = None,
) -> None:
    if not run_directory:
        logger.debug("Not writing function summary for %s", memo_uri)
        return

    invoked_at = metadata.invoked_at if metadata else dt.datetime.utcnow()

    parts = function_memospace.parse_memo_uri(memo_uri, runner_prefix)
    full_function_name = f"{parts.function_module}:{parts.function_name}"
    log_file = _generate_log_filename(run_directory, invoked_at, full_function_name, parts.args_hash)

    log_entry: LogEntry = {
        "function_name": full_function_name,
        "memo_uri": memo_uri,
        "runner_prefix": parts.runner_prefix,
        "pipeline_id": parts.pipeline_id,
        "function_logic_key": parts.function_logic_key,
        "timestamp": invoked_at.isoformat(),
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
    if source_uris := _extract_source_uris(args_kwargs):
        log_entry["uris_in_args_kwargs"] = sorted(source_uris)
    if source_uris := _extract_source_uris(return_value):
        log_entry["uris_in_rvalue"] = sorted(source_uris)

    try:
        assert not log_file.exists(), f"Log file '{log_file}' should not already exist"
        with log_file.open("w") as f:
            json.dump(log_entry, f, indent=2)
    except Exception:
        logger.info(
            f"Unable to write mops function invocation log file at '{log_file}' - you may have multiple callers for the same invocation"
        )
