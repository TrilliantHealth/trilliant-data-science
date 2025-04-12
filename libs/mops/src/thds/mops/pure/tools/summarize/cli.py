import argparse
import json
import statistics
import sys
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
    runner_prefixes: Set[str]
    pipeline_ids: Set[str]
    function_logic_keys: Set[str]
    invoked_by: List[str]
    invoker_code_version: List[str]
    remote_code_version: List[str]
    total_runtime_minutes: List[float]  # minutes
    remote_runtime_minutes: List[float]  # minutes
    uris_in_rvalue: List[str]
    uris_in_args_kwargs: List[str]


def _empty_summary() -> FunctionSummary:
    return {
        "total_calls": 0,
        "cache_hits": 0,
        "executed": 0,
        "timestamps": [],
        "runner_prefixes": set(),
        "pipeline_ids": set(),
        "function_logic_keys": set(),
        "error_count": 0,
        "invoked_by": list(),
        "invoker_code_version": list(),
        "remote_code_version": list(),
        "total_runtime_minutes": list(),
        "remote_runtime_minutes": list(),
        "uris_in_rvalue": list(),
        "uris_in_args_kwargs": list(),
    }


def _process_log_file(log_file: Path) -> Dict[str, FunctionSummary]:
    """
    Process a single JSON log file and return a partial summary.
    :param log_file: Path to the log file
    :return: A dictionary with the function names as keys and their execution summaries as values
    """
    partial_summary: Dict[str, FunctionSummary] = {}
    with log_file.open("r") as f:
        try:
            log_entry: run_summary.LogEntry = json.load(f)
        except json.JSONDecodeError:
            print(f"Error reading log file '{log_file}'")
            return dict()

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
        summary["uris_in_rvalue"].extend(log_entry.get("uris_in_rvalue") or tuple())
        summary["uris_in_args_kwargs"].extend(log_entry.get("uris_in_args_kwargs") or tuple())

        mu_parts = parse_memo_uri(
            log_entry["memo_uri"], runner_prefix=log_entry.get("runner_prefix", "")
        )

        summary["runner_prefixes"].add(mu_parts.runner_prefix)
        summary["pipeline_ids"].add(mu_parts.pipeline_id)
        summary["function_logic_keys"].add(mu_parts.function_logic_key)

        # new metadata stuff below:
        def append_if_exists(key: str) -> None:
            if key in log_entry:
                summary[key].append(log_entry[key])  # type: ignore

        for key in (
            "invoked_by",
            "invoker_code_version",
            "remote_code_version",
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
        acc[function_name]["runner_prefixes"].update(data["runner_prefixes"])
        acc[function_name]["pipeline_ids"].update(data["pipeline_ids"])
        acc[function_name]["function_logic_keys"].update(data["function_logic_keys"])
        acc[function_name]["uris_in_rvalue"].extend(data["uris_in_rvalue"])
        acc[function_name]["uris_in_args_kwargs"].extend(data["uris_in_args_kwargs"])

        for key in (
            "invoked_by",
            "invoker_code_version",
            "remote_code_version",
            "total_runtime_minutes",
            "remote_runtime_minutes",
        ):
            acc[function_name][key].extend(data[key])  # type: ignore

    return acc


def _format_summary(summary: Dict[str, FunctionSummary], sort_by: SortOrder, uri_limit: int = 10) -> str:
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
        "  Runner Prefixes: {runner_prefixes}\n"
        "  Pipeline IDs: {pipeline_ids}\n"
        "  Function Logic Keys: {function_logic_keys}\n"
        "  Function Runtime minutes: {function_runtimes}\n"
        "  Wall clock minutes: {wall_clock_runtimes}\n"
        "  Invoked by: {invokers}\n"
        "  Invoker code versions: {invoker_code_version}\n"
        "  Remote code versions: {remote_code_version}\n"
    )
    report_lines = []

    sorted_items = (
        sorted(summary.items(), key=lambda item: item[0])
        if sort_by == "name"
        else sorted(summary.items(), key=lambda item: min(item[1]["timestamps"]))
    )

    for function_name, data in sorted_items:

        def first_and_last_n(
            obj_set: ty.Collection[str], n: int
        ) -> ty.Tuple[ty.List[str], ty.List[str], int]:
            """take the first n and the last n, unless they would overlap, in which case take the whole list"""
            if len(obj_set) <= n * 2:
                return list(obj_set), list(), 0
            obj_list = list(obj_set)
            return obj_list[:n], obj_list[-n:], len(obj_set) - n * 2

        def and_more(obj_set: ty.Collection[str], max_count: int = 4) -> str:
            if max_count < 1:
                return ""
            if max_count == 1:
                max_count = 2  # stupid, but keeps the code simpler.
            the_first, the_last, remaining_count = first_and_last_n(obj_set, max_count // 2)
            return ", ".join(
                [
                    *the_first,
                    *([f"...skipping {remaining_count} more..."] if remaining_count else list()),
                    *the_last,
                ]
            )

        def describe(fs: FunctionSummary, key: str) -> str:
            numlist: ty.List[float] = fs[key]  # type: ignore
            if not numlist:
                return ""

            avg = sum(numlist) / len(numlist)
            maxi = max(numlist)
            mini = min(numlist)
            pstddev = statistics.pstdev(numlist)
            return f"avg: {avg:.2f}, min: {mini:.2f}, max: {maxi:.2f}, pstdev: {pstddev:.2f}"

        report_lines.append(
            template.format(
                function_name=function_name,
                total_calls=data["total_calls"],
                cache_hits=data["cache_hits"],
                executed=data["executed"],
                error_count=data["error_count"],
                timestamps=and_more(sorted(data["timestamps"])),
                runner_prefixes=and_more(data["runner_prefixes"]),
                pipeline_ids=", ".join(data["pipeline_ids"]),
                function_logic_keys=", ".join(data["function_logic_keys"]),
                function_runtimes=describe(data, "remote_runtime_minutes"),
                wall_clock_runtimes=describe(data, "total_runtime_minutes"),
                invokers=", ".join(sorted(set(data["invoked_by"]))),
                invoker_code_version=", ".join(sorted(set(data["invoker_code_version"]))),
                remote_code_version=", ".join(sorted(set(data["remote_code_version"]))),
            )
        )
        args_uris = and_more(
            sorted(data["uris_in_args_kwargs"]),
            max_count=uri_limit if uri_limit >= 0 else sys.maxsize,
        ).replace(", ", "\n     ")
        if args_uris:
            report_lines.append(f"  URIs in args/kwargs:\n     {args_uris}\n")
        n_uris = and_more(
            sorted(data["uris_in_rvalue"]),
            max_count=uri_limit if uri_limit >= 0 else sys.maxsize,
        ).replace(", ", "\n     ")
        if n_uris:
            report_lines.append(f"  URIs in return value(s):\n     {n_uris}\n")
    return "\n".join(report_lines)


def auto_find_run_directory(start_dir: ty.Optional[Path] = None) -> Path:
    if start_dir is None:
        mops_root = run_summary.MOPS_SUMMARY_DIR()
    else:
        mops_root = start_dir / run_summary.MOPS_SUMMARY_DIR()
    if not mops_root.exists():
        raise ValueError(f"No mops summary root directory found at {mops_root}.")
    if not mops_root.is_dir():
        raise RuntimeError(
            "Mops summary root is not a directory! "
            f"Delete {mops_root} to allow mops to recreate it on the next run."
        )
    for directory in sorted(mops_root.iterdir(), key=lambda x: x.name, reverse=True):
        if directory.is_dir() and list(directory.glob("*.json")):
            # needs to have some files for it to count for anything
            return directory

    raise ValueError(f"No pipeline run directories found in {mops_root}.")


def summarize(
    run_directory: Optional[str] = None, sort_by: SortOrder = "name", uri_limit: int = 10
) -> None:
    run_directory_path = Path(run_directory) if run_directory else auto_find_run_directory()

    print(f"Summarizing pipeline run '{run_directory_path}'\n")
    log_files = list(run_directory_path.glob("*.json"))

    partial_summaries = map(_process_log_file, log_files)

    summary: Dict[str, FunctionSummary] = reduce(_combine_summaries, partial_summaries, {})

    report = _format_summary(summary, sort_by, uri_limit)
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
        default="time",
        help="Sort the summary by function name or by the first call time",
    )
    parser.add_argument(
        "--uri-limit",
        type=int,
        default=10,
        help=(
            "Limit the number of Source URIs printed in the summary for each function."
            " Grep for lines beginning with 5 spaces to get only the URIs."
            " Negative numbers (e.g. -1) mean no limit."
        ),
    )
    args = parser.parse_args()
    try:
        summarize(args.run_directory, args.sort_by, args.uri_limit)
    except ValueError as e:
        print(f"Error: {e}")
