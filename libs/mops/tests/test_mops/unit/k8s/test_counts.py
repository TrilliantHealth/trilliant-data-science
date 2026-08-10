"""Shared launch counters, and the import-time cost of having them."""

import subprocess
import sys

from thds.mops.k8s import counts


def test_importing_does_not_start_a_resource_tracker():
    """Allocating shared memory at import forks multiprocessing's resource tracker, which
    fails outright in a process whose standard file descriptors are not what `posix_spawn`
    expects - a full-screen terminal application, for one. Importing a package must not
    require the process to be forkable.

    Run in a fresh interpreter because the tracker is process-global: anything that touched
    a counter earlier in the session would already have started it.
    """
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "import multiprocessing.resource_tracker as rt;"
            " import thds.mops.k8s.counts;"
            " print(rt._resource_tracker._fd is not None)",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert probe.stdout.strip() == "False"


def test_a_counter_is_allocated_once_and_counts_up():
    counter = counts.LAUNCH_COUNT

    assert counts.LAUNCH_COUNT is counter
    assert counts.inc(counter) == counter.value


def test_the_two_counters_are_distinct():
    assert counts.LAUNCH_COUNT is not counts.FINISH_COUNT


def test_assigning_a_counter_replaces_it():
    """`batching` swaps in counters from its own multiprocessing context, which only works
    because an assignment shadows the lazy lookup permanently."""
    original = counts.LAUNCH_COUNT
    replacement = counts._counter("a-different-counter")
    try:
        counts.LAUNCH_COUNT = replacement  # type: ignore[misc]

        assert counts.LAUNCH_COUNT is replacement
    finally:
        counts.LAUNCH_COUNT = original  # type: ignore[misc]


def test_an_unknown_name_still_raises():
    try:
        counts.NOT_A_COUNTER  # type: ignore[attr-defined]
    except AttributeError:
        return

    raise AssertionError("expected AttributeError for an unknown module attribute")
