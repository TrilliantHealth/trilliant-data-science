from pathlib import Path

import pytest

from thds.mops.k8s import _launch
from thds.mops.pure.tools.console import runtime


def _provider() -> runtime.RuntimeContext:
    return runtime.RuntimeContext("teapot", {"vessel": "short-and-stout", "spout": ""})


def _raising_provider() -> runtime.RuntimeContext:
    raise RuntimeError("no handle")


_HERE = __name__


def test_nothing_configured_describes_nothing():
    """The default for every runtime that never registered one: an execution nobody can
    address, reported as such rather than as empty coordinates."""
    assert runtime.current() == runtime.EMPTY


def test_a_configured_provider_describes_this_process():
    with runtime.RUNTIME_CONTEXT_PROVIDER.set_local(f"{_HERE}._provider"):
        assert runtime.current().runtime == "teapot"
        assert runtime.current().coordinates == {"vessel": "short-and-stout"}
        # the empty spout is dropped: an unset variable is how a coordinate says it does
        # not apply, and a reader should not have to tell those apart from real values.


@pytest.mark.parametrize(
    "import_path",
    [f"{_HERE}._raising_provider", f"{_HERE}.no_such_function", "not.a.module.at.all", "bare"],
)
def test_a_provider_that_cannot_be_used_costs_only_the_address(import_path):
    """By the time this is called the invocation is already running. A provider that is
    missing, unimportable, misspelled, or broken must cost a reader the address of one
    execution and nothing else."""
    with runtime.RUNTIME_CONTEXT_PROVIDER.set_local(import_path):
        assert runtime.current() == runtime.EMPTY


def test_the_launchers_and_the_config_item_agree_on_one_environment_variable():
    """The launchers set `MOPS_RUNTIME_CONTEXT` in the remote's environment; this reads
    whatever its own name derives to. Nothing connects the two but the name, and nothing
    fails when they diverge - a remote simply reports no coordinates, and every row on a
    reader says the work ran nowhere.

    Every other test here sets the provider directly, bypassing the environment entirely,
    which is what let the two drift apart unnoticed.
    """
    assert runtime.RUNTIME_CONTEXT_PROVIDER.envname == "MOPS_RUNTIME_CONTEXT"


def test_the_launcher_exports_the_variable_this_reads():
    """The other half of the correspondence, read off the launcher rather than restated.

    A config item takes its environment variable from its name, and the value is read when
    the item is created - so this cannot be exercised by setting the variable here, and
    the two ends can only be checked against each other.
    """
    launcher = Path(_launch.__file__).read_text()

    assert f'add_env_var("{runtime.RUNTIME_CONTEXT_PROVIDER.envname}"' in launcher
