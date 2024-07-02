import argparse
import json
from functools import reduce
from pathlib import Path
from typing import Dict, List, Literal, Optional, TypedDict

from thds.mops.pure.tools.summarize import run_summary

SortOrder = Literal["name", "time"]


class FunctionSummary(TypedDict):
    total_calls: int
    cache_hits: int
    executed: int
    timestamps: List[str]


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
            partial_summary[function_name] = {
                "total_calls": 0,
                "cache_hits": 0,
                "executed": 0,
                "timestamps": [],
            }
        partial_summary[function_name]["total_calls"] += 1
        if log_entry["status"] in ("memoized", "awaited"):
            partial_summary[function_name]["cache_hits"] += 1
        else:
            partial_summary[function_name]["executed"] += 1
        partial_summary[function_name]["timestamps"].append(log_entry["timestamp"])

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
            acc[function_name] = {"total_calls": 0, "cache_hits": 0, "executed": 0, "timestamps": []}
        acc[function_name]["total_calls"] += data["total_calls"]
        acc[function_name]["cache_hits"] += data["cache_hits"]
        acc[function_name]["executed"] += data["executed"]
        acc[function_name]["timestamps"].extend(data["timestamps"])
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
        "  Timestamps: {timestamps}\n"
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

        report_lines.append(
            template.format(
                function_name=function_name,
                total_calls=data["total_calls"],
                cache_hits=data["cache_hits"],
                executed=data["executed"],
                timestamps=timestamps_str,
            )
        )
    return "\n".join(report_lines)


def summarize(run_directory: Optional[str] = None, sort_by: SortOrder = "name") -> None:
    mops_root = run_summary.MOPS_SUMMARY_DIR()
    if not mops_root.exists():
        raise ValueError(f"No mops summary root directory found at {mops_root}")
    if not mops_root.is_dir():
        raise RuntimeError(
            "Mops summary root is not a directory! "
            f"Delete {mops_root} to allow mops to recreate it on the next run."
        )
    if run_directory:
        run_directory_path = Path(run_directory)
    else:
        run_directories = sorted(mops_root.iterdir(), key=lambda x: x.name, reverse=True)
        if not run_directories:
            print("No pipeline run directories found.")
            return
        run_directory_path = run_directories[0]

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
