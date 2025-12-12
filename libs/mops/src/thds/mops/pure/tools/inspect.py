"""Inspect mops control files and unpickle them for debugging.

Note that this really only works with ADLS-like Blob Stores, and
only with the MemoizingPicklingRunner, which is the only implementation
we have as of 2024-09-24, and will probably be the only implementation ever...
but if you're reading this in the distant future - those are its limitations.
"""

import argparse
import concurrent.futures
import functools
import io
import json
import os
import re
import subprocess
import sys
import typing as ty
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from pprint import pprint

from thds import adls
from thds.core import log, parallel, scope, thunks, tmp
from thds.mops.parallel import Thunk
from thds.mops.pure.core import uris
from thds.mops.pure.core.memo import results
from thds.mops.pure.pickling._pickle import (
    CallableUnpickler,
    read_metadata_and_object,
    unfreeze_args_kwargs,
)
from thds.mops.pure.pickling.pickles import Invocation
from thds.mops.pure.runner import strings

from . import _pickle_dis

logger = log.getLogger(__name__)


class _MopsInspectPrettyPartial(functools.partial):
    def __repr__(self) -> str:
        return f"partial({self.func.__name__}, {self.args}, {self.keywords})"

    def __rich_repr__(self) -> ty.Iterable[ty.Tuple[str, ty.Any]]:
        """I don't much like how partial does its repr. Especially with nested partials,
        it becomes almost impossible to follow.
        """
        yield "function", self.func.__name__
        yield "args", self.args
        yield "keywords", self.keywords


class PartialViewingUnpickler(CallableUnpickler):
    def find_class(self, module: str, name: str) -> ty.Any:
        if module == "functools" and name == "partial":
            return _MopsInspectPrettyPartial
        return super().find_class(module, name)


def _unpickle_object_for_debugging(uri: str) -> ty.Any:
    try:
        if uri.endswith("/" + strings.INVOCATION):
            _no_header, invoc_raw = read_metadata_and_object(strings.INVOCATION, uri)
            invoc = ty.cast(Invocation, invoc_raw)
            args, kwargs = unfreeze_args_kwargs(invoc.args_kwargs_pickle, PartialViewingUnpickler)
            return Thunk(getattr(invoc, "f", None) or invoc.func, *args, **kwargs)

        header, obj = read_metadata_and_object("output", uri)
        return obj, header
    except ImportError as ie:
        logger.error(f"Could not import the module ({ie}) needed to unpickle the object.")
        logger.error("Try re-running this tool in the environment where the above module is available.")
        raise


def _resolved_uri(uri: str) -> str:
    if not uri:
        return ""
    if fqn := adls.uri.resolve_uri(uri):
        return str(fqn)
    return uri


_KNOWN_CONTROL_FILES = [strings.INVOCATION, results.RESULT, results.EXCEPTION]

# prefix with forward-slash because these live in a blob store 'directory'


@dataclass
class IRE:
    invocation: ty.Any
    result: ty.Any  # a.k.a. return_value
    exception: ty.Any


_NOTHING = object()


def _control_uri(uri: str) -> str:
    for control_file in _KNOWN_CONTROL_FILES:
        if uri.endswith("/" + control_file):
            return control_file
    return ""


@scope.bound
def get_control_file(uri: str, unpickle: bool = True) -> ty.Any:
    """Returns _NOTHING if 'normal' errors occur."""
    try:
        uri = _resolved_uri(uri)
    except Exception as e:
        logger.error(f"Error while resolving {uri}: {e}")
        return _NOTHING

    if not _control_uri(uri):
        fs = uris.lookup_blob_store(uri)
        logger.debug(f"Attempting to fetch all control files for {uri}")
        return IRE(
            **{cf: get_control_file(fs.join(uri, cf), unpickle=unpickle) for cf in _KNOWN_CONTROL_FILES}
        )

    has_storage_root = bool(uris.ACTIVE_STORAGE_ROOT())
    try:
        scope.enter(uris.ACTIVE_STORAGE_ROOT.set(uris.get_root(uri)))
        if unpickle:
            return _unpickle_object_for_debugging(uri)
        else:
            return _pickle_dis.get_meta_and_pickle(uri)
    except ImportError:
        return None
    except Exception as e:
        if uris.lookup_blob_store(uri).is_blob_not_found(e):
            if has_storage_root or uri not in str(e):
                logger.warning(str(e))
            return None
        logger.exception(
            f"Unexpected error {e} while {'unpickling' if unpickle else 'processing'} the object at {uri}"
        )
        raise


def _embed(o: object) -> None:
    print('\nObject will be available as "o". Perform embedded URI fetches with "get_control_file"\n')
    try:
        __import__("IPython").embed()
    except ImportError:
        print("IPython not found, falling back to standard Python shell.")
        import code

        code.interact(local=locals())


def _pprint(obj: object, file: ty.Any = None, uri: str = "") -> None:
    final_out_stream = file or sys.stdout

    if uri:
        print(uri, file=final_out_stream)

    # Always capture the pretty-printed output to an in-memory buffer first
    output_buffer = io.StringIO()

    try:
        # Attempt to use rich for pretty-printing into the buffer
        from rich import console, pretty  # type: ignore[import-not-found]

        console.Console(file=output_buffer, color_system=None).print(pretty.Pretty(obj), crop=False)
    except ModuleNotFoundError:
        pprint(obj, indent=4, width=60, sort_dicts=False, stream=output_buffer)

    formatted_string = output_buffer.getvalue()
    # Unescape the literal '\n' sequences into actual newlines
    processed_string = re.sub(r"(?<!\\)\\n", "\n", formatted_string)

    # Use print with end='' for stdout to avoid double newlines
    if final_out_stream is sys.stdout:
        print(processed_string, end="")
    else:
        final_out_stream.write(processed_string)


def inspect(uri: str, embed: bool = False) -> ty.Any:
    obj = get_control_file(uri)
    if obj is _NOTHING:
        return

    if embed:
        _embed(obj)
    else:
        print()
        _pprint(obj)
    return obj


def inspect_and_log(memo_uri: str) -> None:
    inspect(memo_uri)
    logger.error(
        "A required result was not found."
        " You can compare the above output with other invocations"
        f" by running `mops-inspect {memo_uri}`"
        " in your local Python environment."
    )


@dataclass
class Ignores:
    permanent_ignores_file: Path
    known_ignores: ty.Set[str]

    def __post_init__(self) -> None:
        self.permanent_ignores_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.permanent_ignores_file.exists():
            self.permanent_ignores_file.touch()
        self.known_ignores = set(filter(None, open(self.permanent_ignores_file).read().splitlines()))

    def ignore_uri(self, ignore_uri: str) -> None:
        self.known_ignores.add(ignore_uri)
        # possible race condition here if multiple runs of mops-inspect are happening
        # in parallel?
        with open(self.permanent_ignores_file, "a") as wf:
            wf.write(ignore_uri + "\n")

    def __contains__(self, uri: str) -> bool:
        return uri in self.known_ignores


@dataclass
class Matches:
    must_match: ty.List[str]
    must_not_match: ty.List[str]

    def add_regex(self, regex: str) -> ty.Literal["ignore", "match"]:
        """These are not permanent"""
        if regex.startswith("!"):
            self.must_not_match.append(regex[1:])
            return "ignore"

        self.must_match.append(regex)
        return "match"

    def matches(self, ire_str: str) -> bool:
        for regex in self.must_not_match:
            if re.search(regex, ire_str):
                logger.debug('Ignoring because of regex: "%s"', regex)
                return False

        if not self.must_match:
            logger.debug("No regexes must match")
            return True

        all_match = all(re.search(regex, ire_str) for regex in self.must_match)
        if all_match:
            logger.debug("Matches all required regexes")
            return True

        logger.debug("Does not match all of the %d required regexes.", len(self.must_match))
        return False


_IGNORES = Ignores(Path("~/.mops-inspect-ignores").expanduser(), set())
_MATCHES = Matches(list(), list())
DIFF_TOOL = os.environ.get("DIFF_TOOL") or "difft"  # nicer diffs by default


def _check_diff_tool() -> None:
    global DIFF_TOOL
    try:
        subprocess.run([DIFF_TOOL, "--version"], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        logger.warning("You may want to `brew install difft` for nicer diffs.")
        DIFF_TOOL = "diff"


def _run_diff_tool(path_old: Path, path_new: Path) -> None:
    subprocess.run([DIFF_TOOL, str(path_old), str(path_new)], check=True)


def _write_ire_to_path(ire: IRE, path: Path, uri: str) -> None:
    with open(path, "w") as wf:
        _pprint(ire, file=wf, uri=uri)


@scope.bound
def pickle_diff_two_uris(uri1: str, uri2: str) -> None:
    """Diff two pickled objects, using the diff tool specified in DIFF_TOOL."""
    _check_diff_tool()
    uri1 = _resolved_uri(uri1)
    uri2 = _resolved_uri(uri2)

    path1 = scope.enter(tmp.temppath_same_fs())
    path2 = scope.enter(tmp.temppath_same_fs())

    ire1 = get_control_file(uri1, unpickle=False)
    ire2 = get_control_file(uri2, unpickle=False)

    _write_ire_to_path(ire1, path1, uri1)
    _write_ire_to_path(ire2, path2, uri2)

    _run_diff_tool(path1, path2)


def _diff_uris(uri: str, other_uris: ty.Collection[str]) -> ty.List[str]:
    """Interactively diff the control file at `uri` against all others in `other_uris`."""
    base_uri_path = scope.enter(tmp.temppath_same_fs())
    _write_ire_to_path(get_control_file(uri), base_uri_path, uri)

    def uri_menu(other_uri: str) -> ty.Optional[str]:
        uri_choices = (
            "\n    i to permanently ignore this URI,\n    p to 'pick' this URI for later review,"
        )

        choice = input(
            "\nEnter to continue,"
            "\n    q or Ctrl-C to quit,"
            + (uri_choices if other_uri else "")
            + "\n or type a regex to filter future results"
            + " (prefix with ! to find non-matches, otherwise will find matches): "
        )
        if "i" == choice.lower():
            if other_uri:
                _IGNORES.ignore_uri(other_uri)
        elif "p" == choice.lower():
            if other_uri:
                print(f"\nPicked URI: {other_uri}")
                uri_menu("")  # re-prompt for regex
                return other_uri
            return ""

        elif choice.lower() in {"q", "quit"}:
            print("\n\nExiting diff loop.\n\n")
            raise KeyboardInterrupt()
        elif choice:
            regex = choice
            type = _MATCHES.add_regex(regex)
            logger.info(f"Added <{type}> regex /{regex}/")
        print()
        return None

    fs = uris.lookup_blob_store(uri)
    control_type = _control_uri(uri)
    other_uris = [fs.join(o_uri, control_type) for o_uri in other_uris]
    other_uris = [o_uri for o_uri in other_uris if o_uri not in _IGNORES and o_uri != uri]

    uris_picked = []
    # prefetch_all_control_files
    log.getLogger("thds.adls").setLevel("WARN")
    for other_uri, control_file in parallel.yield_all(
        [(other_uri, thunks.thunking(get_control_file)(other_uri)) for other_uri in other_uris],
        executor_cm=concurrent.futures.ThreadPoolExecutor(max_workers=8),
    ):
        with tmp.temppath_same_fs() as path_for_other_uri:
            _write_ire_to_path(control_file, path_for_other_uri, other_uri)  # type: ignore
            if not _MATCHES.matches(path_for_other_uri.read_text()):
                continue

            _run_diff_tool(path_for_other_uri, base_uri_path)

        try:
            if uri_menu(other_uri):
                uris_picked.append(other_uri)
        except KeyboardInterrupt:
            break

    return uris_picked


def _diff_memospace(uri: str) -> ty.List[str]:
    """Diff all siblings in the memospace against the new invocation.

    Ignore any that have been ignored previously.
    """
    # this code operates on the assumption that you've provided
    # it with the 'new' invocation, and you're trying to figure out
    # what is 'new' as compared to other 'existing' (old) invocations.
    # Therefore, the 'green' highlighted text will be the 'new' invocation,
    # and the red will be all the old ones that we loop over below.
    fs = uris.lookup_blob_store(uri)
    memospace_uri = fs.join(*fs.split(uri)[: -2 if _control_uri(uri) else -1])
    # go up two levels to find the memospace if necessary.

    logger.info(f"Diffing against all siblings in the memospace {memospace_uri}")

    sibling_uris = fs.list(memospace_uri)  # type: ignore
    sibling_uris = [s_uri for s_uri in sibling_uris if not uri.startswith(s_uri)]
    # eliminate the URI itself - but we use prefix match because we might have specified invocation or something.

    if not sibling_uris:
        logger.warning(
            f"No memospace siblings found for '{memospace_uri}'"
            " - check your pipeline ID, function-logic-key (if any),"
            " and whether you're running in prod or dev."
        )
        return []

    return _diff_uris(uri, sibling_uris)


def _load_run_summary_dir(run_dir: ty.Optional[str] = None) -> ty.List[ty.Dict[str, ty.Any]]:
    from thds.mops.pure.tools.summarize.cli import auto_find_run_directory

    run_dir_path = Path(run_dir or auto_find_run_directory())
    logger.info(f'Diffing against memo URIs found in run directory: "{run_dir_path}"')
    log_files = list(run_dir_path.glob("*.json"))
    return [json.loads(log_file.read_text()) for log_file in log_files]


def _diff_summary(
    base_memo_uri: str,
    run_dir: ty.Optional[str] = None,
) -> ty.List[str]:
    """Return cut-down summary"""

    run_summaries = _load_run_summary_dir(run_dir)

    # now order by which ones have the most similar prefix... though ideally we'd be
    # taking into account the total length of common subsequences
    def _common_prefix_len(s1: str, s2: str) -> int:
        common_len = 0
        for c1, c2 in zip(s1, s2):
            if c1 != c2:
                break
            common_len += 1
        return common_len

    uris = [rs["memo_uri"] for rs in run_summaries]
    uris = sorted(uris, key=functools.partial(_common_prefix_len, base_memo_uri), reverse=True)

    return _diff_uris(base_memo_uri, uris)


@scope.bound
def _inspect_uri(uri: str, embed: bool) -> None:
    inspect(_resolved_uri(uri), embed)  # print the main uri


def _write_picked_uris(picked_uris: ty.List[str]) -> None:
    if picked_uris:
        outfile = Path(".mops/inspect") / f"picked_uris-{datetime.now().isoformat()}.txt"
        outfile.parent.mkdir(parents=True, exist_ok=True)
        with open(outfile, "w") as f:
            f.write("\n".join(picked_uris) + "\n")
        print(f"Wrote picked URIs to {outfile} for later review.")


def main() -> None:
    from thds.mops.pure.tools.summarize.run_summary import RUN_NAME

    RUN_NAME.set_global("")  # disable run summary logging for this tool

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "uri",
        type=str,
        help="The URI of the first object to inspect. Can be adls:// or https:// or even abfss://",
    )
    parser.add_argument(
        "--diff-summary",
        "-s",
        nargs="?",  # Makes the value optional
        const="defaultdir",  # Value assigned if flag is present but no value is given
        default=None,  # Value assigned if flag is not present
        help="Diff against all memo URIs found in the run summary directory. Emit a file listing the URIs compared.",
    )
    parser.add_argument(
        "--diff-picked",
        "-p",
        type=Path,
        default=None,
        help="Diff against previously picked URIs (from newline-separated text file) from a prior run of this tool.",
    )
    parser.add_argument(
        "--diff-memospace",
        "-d",
        action="store_true",
        help=(
            "Find the diff between the invocation at the provided URI,"
            " and all other invocations that match the same function memospace."
            " This will only work if your Blob Store is capable of listing files."
            " It is highly recommended that you `brew install difftastic` to get more precise diffs."
        ),
    )
    parser.add_argument(
        "--diff-pickle-ops",
        help="""Diff against the provided memo URI, but emit pickle opcodes rather than unpickling.""",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Keep prompting for URIs to inspect - basically just an embedded while loop.",
    )
    parser.add_argument("--embed", action="store_true", help="Embed an IPython shell after inspection.")
    args = parser.parse_args()
    args.uri = args.uri.rstrip("/")
    if args.diff_memospace or args.diff_pickle_ops or args.diff_summary:
        _check_diff_tool()

    if args.diff_picked:
        _write_picked_uris(
            _diff_uris(
                args.uri,
                [line.strip() for line in open(args.diff_picked).read().splitlines() if line.strip()],
            )
        )
    elif args.diff_pickle_ops:
        pickle_diff_two_uris(args.uri, args.diff_pickle_ops)
    elif args.diff_summary:
        _write_picked_uris(
            _diff_summary(args.uri, None if args.diff_summary == "defaultdir" else args.diff_summary)
        )
    elif args.diff_memospace:
        _write_picked_uris(_diff_memospace(args.uri))
    else:
        _inspect_uri(args.uri, args.embed)

    if args.loop:
        prompt = "\nEnter another URI to inspect, or empty string to exit: "
        uri = input(prompt)
        while uri:
            if args.diff_pickle_ops:
                pickle_diff_two_uris(args.uri, uri)
            else:
                _inspect_uri(uri, args.embed)
            uri = input(prompt)


if __name__ == "__main__":
    main()
