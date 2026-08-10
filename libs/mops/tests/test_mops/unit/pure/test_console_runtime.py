import pytest

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
