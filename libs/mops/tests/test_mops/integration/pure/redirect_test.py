"""Redirect is sort of a catch-all concept and this tests that it can
do some pretty interesting stuff.
"""
import logging
from functools import partial

from thds.core.stack_context import StackContext
from thds.mops.pure import MemoizingPicklingRunner
from thds.mops.pure.pickling.memoize_only import _threadlocal_shell

from ...config import TEST_TMP_URI

_HIDDEN_CONTEXT = StackContext("hidden", 2)


def foobar(i: int) -> int:
    return i * _HIDDEN_CONTEXT()


def foobar_redirect(hidden: int, i: int) -> int:
    with _HIDDEN_CONTEXT.set(hidden):
        return foobar(i)


def test_redirect_allows_pickling_of_context_without_affecting_invocation_memoization(caplog):
    """This is a weird test.

    The core goal is simply to prove that 'things' can be passed to
    the underlying function, using the redirect functionality, without
    affecting memoization.

    The basic idea is that because you can _actually_ call a different
    function than the one you 'promised' to call, the actual function
    that gets called can be a partial with additional context that, in
    this case, does _not_ get factored into the memoization key.

    """
    caplog.set_level(logging.INFO)

    runner = MemoizingPicklingRunner(_threadlocal_shell, TEST_TMP_URI)
    # run foobar 'directly' via the runner
    assert runner(foobar, (1,), dict()) == 2
    assert not any("already exists" in rec.message for rec in caplog.records)
    # no memoization occurred

    assert foobar_redirect(3, 1) == 3
    # first prove that this is what we'd _expect_ to get if memoization weren't occurring

    # now run foobar via the redirect, and demonstrate that it has the same cache key
    # (since this hidden context is excluded from all parts of the memo key computation)
    # and therefore it gets the 'wrong' result. The goal is simply to demonstrate that
    # we can pass context that does not factor into the memoization at all. A real world
    # use case would be passing something that changes side effects but does not affect the real result.
    runner = MemoizingPicklingRunner(
        _threadlocal_shell, TEST_TMP_URI, redirect=lambda f, _a, _k: partial(foobar_redirect, 3)  # not 2
    )
    assert runner(foobar, (1,), dict()) == 2
    # still 2 because we got a memoized result not affected by the
    # fact that we tried to pass 3 through as the hidden context.
    assert any("already exists" in rec.message for rec in caplog.records)
    # memoization occurred
