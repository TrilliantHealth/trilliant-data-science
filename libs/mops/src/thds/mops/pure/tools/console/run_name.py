"""Names the orchestrator run that a set of events belongs to.

Distinct from mops's existing `run_id`, which identifies a single remote execution and
appears in output paths and `result-metadata-<run_id>.txt`. One run name covers every
invocation an orchestrator dispatches - thousands of them, each with its own `run_id`.

The `mr.` prefix keeps these visually distinct from other humenc strings (lease writer ids,
result run ids) when they appear side by side in a URI or a log line.
"""

import datetime as dt
import os

from thds.core import config
from thds.humenc import temporal

RUN_NAME = config.item("thds.mops.console.run_name", default="", parse=str)
# settable so a caller can name a run deliberately, and so child processes inherit the
# parent's name through the environment rather than minting their own.

_PREFIX = "mr."


def day_of(at: dt.datetime) -> str:
    """The directory a run started on, which is how runs are found later.

    Runs are looked for by when they happened, and a flat prefix holding every run ever
    started answers that question only by listing all of it. Older days can be rolled up
    into month directories without the name itself having to change.
    """
    return at.astimezone(dt.timezone.utc).strftime("%Y-%m-%d")


def generate(at: None | dt.datetime = None) -> str:
    """A time-sortable, unguessable name for one orchestrator run.

    Sorting only has to hold within a day, because the day is in the path - so the
    temporal bits span one day rather than humenc's default two-month cycle, which put
    every run started on the same day in one bucket. Six bits across a day is about a
    twenty-minute resolution, and the remaining bits stay random so two runs starting in
    the same minute cannot collide.
    """
    at = at or dt.datetime.now(dt.timezone.utc)
    midnight = at.astimezone(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return _PREFIX + temporal.encode_temporal(
        6, now=at, cycle_start=midnight, cycle_length=dt.timedelta(days=1)
    )


def current(enabled: bool) -> str:
    """The run name for this process, minted once and inherited by children.

    Returns empty when the console is turned off - no name is minted, and remotes receive
    an empty value that tells them not to report either. `enabled` is passed rather than
    read here so this module stays independent of where events happen to be written.

    Carries its own day (`2026-08-09/mr.Name`), so any process holding the name can build
    the same path without being told when the run started - which a child that inherited
    only the name otherwise could not do, and would file its events under its own day.
    """
    if not enabled:
        return ""

    if not RUN_NAME():
        now = dt.datetime.now(dt.timezone.utc)
        RUN_NAME.set_global(f"{day_of(now)}/{generate(now)}")
        os.environ.setdefault(RUN_NAME.envname, RUN_NAME())
        # spawned children read this from the environment, so every process in one
        # orchestrator run reports under the same name.

    return RUN_NAME()


def claim(enabled: bool = True) -> str:
    """Mint this run's name now, before anything forks.

    A process pool's workers each import mops fresh and would each mint their own name if
    none existed yet, scattering one run across as many directories as there are workers.
    The environment only propagates a name that exists at fork time, so an orchestrator
    that parallelises its dispatch has to claim one first.

    Idempotent, and safe to call when the console is off - it returns empty and mints
    nothing, exactly as `current` does.
    """
    name = current(enabled)
    if name:
        from . import run_metadata

        run_metadata.claim(name)
    return name
