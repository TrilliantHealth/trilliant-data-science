"""One pointer per run in a day-level index, so a day's runs can be found by listing one prefix.

`<blob root>/mops/console/<day>/_index/<HHMMSS>Z--<label>--<run name>` - the start time (UTC,
like the day), a label, and the run's own name, so a listing of the index is a readable
table of the day's runs without opening anything. The pointer's body is the run's events
root.

The label is what an application says its run is, via `label_run` - `nightly-2026-09`,
`weekly-2026-09` - and falls back to who started it and where. It need not be unique;
the run name is, and the time tells two of the same label apart.
"""

import datetime as dt
import re
import threading

from thds.core import log

from ...core import uris

logger = log.getLogger(__name__)

INDEX_DIRNAME = "_index"

_LABEL_LOCK = threading.Lock()
_LABEL = ""
_PUBLISHED_AS = ""
_WARNED: set[str] = set()


def _warn_once(message: str) -> None:
    if message not in _WARNED:
        _WARNED.add(message)
        logger.warning(message)


def label_run(label: str) -> None:
    """Name this process's run for whoever looks for it later. Call before the first
    mops invocation; a label set after the run has been published takes no effect.

    The first label wins: an outer wrapper that labels its run is not overridden by
    something it calls that labels too, and a disagreement is logged once.
    """
    global _LABEL
    with _LABEL_LOCK:
        if _PUBLISHED_AS:
            if label != _PUBLISHED_AS:
                _warn_once(
                    f"label_run({label!r}) took no effect: the run was published as {_PUBLISHED_AS!r}"
                    " when its first invocation went out. Call it before the first mops call."
                )
            return

        if _LABEL and _LABEL != label:
            _warn_once(f"The run is already labelled {_LABEL!r}; ignoring {label!r}.")
            return

        _LABEL = label


def freeze_label(fallback: str) -> str:
    """The label this run is published under, fixed on first use so every root of the
    run agrees: the one `label_run` set, else `fallback`."""
    global _PUBLISHED_AS
    with _LABEL_LOCK:
        if not _PUBLISHED_AS:
            _PUBLISHED_AS = _LABEL or fallback

        return _PUBLISHED_AS


def _safe(part: str) -> str:
    return re.sub(r"[^A-Za-z0-9._@-]+", "_", part).strip("_") or "run"


def entry_name(started_at: dt.datetime, label: str, run_name: str) -> str:
    """`185651Z--nightly-2026-09--mr.SolidEat.ulewXQ`: sorts by start within the day."""
    stamp = started_at.astimezone(dt.timezone.utc).strftime("%H%M%SZ")
    return f"{stamp}--{_safe(label)}--{_safe(run_name.rsplit('/', 1)[-1])}"


def publish(events_root: str, started_at: dt.datetime, label: str, run_name: str) -> None:
    """Point at a run from its day's index. The run's events root is `<day>/<run name>`,
    so the index sits beside the run."""
    blob_store = uris.lookup_blob_store(events_root)
    day_dir = events_root.rstrip("/").rsplit("/", 1)[0]
    blob_store.putbytes(
        blob_store.join(day_dir, INDEX_DIRNAME, entry_name(started_at, label, run_name)),
        (events_root + "\n").encode(),
        type_hint="text/plain",
    )


def _reset_for_test() -> None:
    global _LABEL, _PUBLISHED_AS
    _LABEL = ""
    _PUBLISHED_AS = ""
    _WARNED.clear()
