import argparse
import json
import typing as ty
from functools import reduce
from pathlib import Path
from typing import Dict, List, Literal, Optional, Set, TypedDict

from thds.mops.pure.core.memo.function_memospace import parse_memo_uri
from thds.mops.pure.tools.summarize import run_summary

SortOrder = Literal["name", "time"]


class FunctionSummary(TypedDict):
    total_calls: int
    cache_hits: int
    executed: int
    error_count: int
    timestamps: List[str]
    memospaces: Set[str]
    pipeline_ids: Set[str]
    function_logic_keys: Set[str]
    invoked_by: List[str]
    invoker_code_versions: List[str]
    remote_code_versions: List[str]
    total_runtime_minutes: List[float]  # minutes
    remote_runtime_minutes: List[float]  # minutes


def _empty_summary() -> FunctionSummary:
    return {
        "total_calls": 0,
        "cache_hits": 0,
        "executed": 0,
        "timestamps": [],
        "memospaces": set(),
        "pipeline_ids": set(),
        "function_logic_keys": set(),
        "error_count": 0,
        "invoked_by": list(),
        "invoker_code_versions": list(),
        "remote_code_versions": list(),
        "total_runtime_minutes": list(),
        "remote_runtime_minutes": list(),
    }


def _process_log_file(log_file: Path) -> Dict[str, FunctionSummary]:
    """
    Process a single JSON log file and return a partial summary.
    :param log_file: Path to the log file
    :return: A dictionary with the function names as keys and their execution summaries as values
    """
    partial_summary: Dict[str, FunctionSummary] = {}
    with log_file.open("r") as f:
        log_entry: run_summary.LogEntry = json.load(f)

        function_name = log_entry["function_name"]
        if function_name not in partial_summary:
            partial_summary[function_name] = _empty_summary()

        summary = partial_summary[function_name]

        summary["total_calls"] += 1
        if log_entry["status"] in ("memoized", "awaited"):
            summary["cache_hits"] += 1
        else:
            summary["executed"] += 1
        summary["error_count"] += int(log_entry.get("was_error") or 0)
        summary["timestamps"].append(log_entry["timestamp"])

        mu_parts = parse_memo_uri(log_entry["memo_uri"], log_entry.get("memospace") or "")

        summary["memospaces"].add(mu_parts.memospace)
        summary["pipeline_ids"].add(mu_parts.pipeline_id)
        summary["function_logic_keys"].add(mu_parts.function_logic_key)

        # new metadata stuff below:
        def append_if_exists(key: str) -> None:
            if key in log_entry:
                summary[key].append(log_entry[key])  # type: ignore

        for key in (
            "invoked_by",
            "invoker_code_versions",
            "remote_code_versions",
            "total_runtime_minutes",
            "remote_runtime_minutes",
        ):
            append_if_exists(key)

    return partial_summary


def _combine_summaries(
    acc: Dict[str, FunctionSummary], partial: Dict[str, FunctionSummary]
) -> Dict[str, FunctionSummary]:
    """
    Combine two summaries into one
    :param acc: the accumulator summary
    :param partial: A partial summary to be combined with the accumulator
    :return: the combined summary
    """
    for function_name, data in partial.items():
        if function_name not in acc:
            acc[function_name] = _empty_summary()
        acc[function_name]["total_calls"] += data["total_calls"]
        acc[function_name]["cache_hits"] += data["cache_hits"]
        acc[function_name]["executed"] += data["executed"]
        acc[function_name]["error_count"] += data["error_count"]
        acc[function_name]["timestamps"].extend(data["timestamps"])
        acc[function_name]["memospaces"].update(data["memospaces"])
        acc[function_name]["pipeline_ids"].update(data["pipeline_ids"])
        acc[function_name]["function_logic_keys"].update(data["function_logic_keys"])

        for key in (
            "invoked_by",
            "invoker_code_versions",
            "remote_code_versions",
            "total_runtime_minutes",
            "remote_runtime_minutes",
        ):
            acc[function_name][key].extend(data[key])  # type: ignore

    return acc


def _format_summary(summary: Dict[str, FunctionSummary], sort_by: SortOrder) -> str:
    """
    Format a summary into a readable report
    """
    template = (
        "Function '{function_name}':\n"
        "  Total calls: {total_calls}\n"
        "  Cache hits: {cache_hits}\n"
        "  Executed: {executed}\n"
        "  Error count: {error_count}\n"
        "  Timestamps: {timestamps}\n"
        "  Memospaces: {memospaces}\n"
        "  Pipeline IDs: {pipeline_ids}\n"
        "  Function Logic Keys: {function_logic_keys}\n"
        "  Avg total runtime: {avg_total_runtime_minutes}m\n"
        "  Avg remote runtime: {avg_remote_runtime_minutes}m\n"
        "  Invoked by: {invokers}\n"
        "  Invoker code versions: {invoker_code_versions}\n"
        "  Remote code versions: {remote_code_versions}\n"
    )
    report_lines = []

    sorted_items = (
        sorted(summary.items(), key=lambda item: item[0])
        if sort_by == "name"
        else sorted(summary.items(), key=lambda item: min(item[1]["timestamps"]))
    )

    for function_name, data in sorted_items:
        timestamps = data["timestamps"]
        if len(timestamps) > 3:
            displayed_timestamps = ", ".join(timestamps[:3])
            remaining_count = len(timestamps) - 3
            timestamps_str = f"{displayed_timestamps}, and {remaining_count} more..."
        else:
            timestamps_str = ", ".join(timestamps)

        def avg(fs: FunctionSummary, key: str) -> float:
            return (sum(fs[key]) / len(fs[key])) if len(fs[key]) else 0  # type: ignore

        report_lines.append(
            template.format(
                function_name=function_name,
                total_calls=data["total_calls"],
                cache_hits=data["cache_hits"],
                executed=data["executed"],
                error_count=data["error_count"],
                timestamps=timestamps_str,
                memospaces=", ".join(data["memospaces"]),
                pipeline_ids=", ".join(data["pipeline_ids"]),
                function_logic_keys=", ".join(data["function_logic_keys"]),
                avg_total_runtime_minutes=avg(data, "total_runtime_minutes"),
                avg_remote_runtime_minutes=avg(data, "remote_runtime_minutes"),
                invokers=", ".join(sorted(set(data["invoked_by"]))),
                invoker_code_versions=", ".join(sorted(set(data["invoker_code_versions"]))),
                remote_code_versions=", ".join(sorted(set(data["remote_code_versions"]))),
            )
        )
    return "\n".join(report_lines)


def _auto_find_run_directory() -> ty.Optional[Path]:
    mops_root = run_summary.MOPS_SUMMARY_DIR()
    if not mops_root.exists():
        raise ValueError(f"No mops summary root directory found at {mops_root}")
    if not mops_root.is_dir():
        raise RuntimeError(
            "Mops summary root is not a directory! "
            f"Delete {mops_root} to allow mops to recreate it on the next run."
        )
    for directory in sorted(mops_root.iterdir(), key=lambda x: x.name, reverse=True):
        if directory.is_dir() and list(directory.glob("*.json")):
            # needs to have some files for it to count for anything
            return directory

    print("No pipeline run directories found.")
    return None


def summarize(run_directory: Optional[str] = None, sort_by: SortOrder = "name") -> None:
    run_directory_path = Path(run_directory) if run_directory else _auto_find_run_directory()
    if not run_directory_path:
        return

    log_files = list(run_directory_path.glob("*.json"))

    partial_summaries = map(_process_log_file, log_files)

    summary: Dict[str, FunctionSummary] = reduce(_combine_summaries, partial_summaries, {})

    report = _format_summary(summary, sort_by)
    print(report)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize mops pipeline run logs.")
    parser.add_argument(
        "run_directory",
        nargs="?",
        type=str,
        default=None,
        help="Path to the pipeline run directory. If not provided, the latest run directory will be used.",
    )
    parser.add_argument(
        "--sort-by",
        choices=["name", "time"],
        default="name",
        help="Sort the summary by function name or by the first call time",
    )
    args = parser.parse_args()
    summarize(args.run_directory, args.sort_by)
