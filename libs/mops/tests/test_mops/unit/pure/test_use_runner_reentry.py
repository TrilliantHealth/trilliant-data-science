"""Tests for use_runner re-entry semantics.

`unwrap_use_runner` exists so the runner's remote entry point can invoke the
just-deserialized user function without re-dispatching back through mops. The
bypass is *keyed to the unwrap target's identity* and *one-shot*: the next
wrapped call whose function matches the unwrap target runs directly; once
that call consumes the bypass, further calls (recursive or otherwise) re-enter
the runner normally. Calls to OTHER wrapped functions never burn the bypass.

These tests pin that contract — without keying, a recursive @pure.magic function
(like the docs/fibonacci.py quickstart) memoizes only the outermost call; without
one-shot, an outer mops function's body that calls a different inner mops function
would bypass the inner call's runner dispatch.
"""

import typing as ty

from thds.mops.pure.core.types import Runner
from thds.mops.pure.core.use_runner import unwrap_use_runner, use_runner


class _CountingRunner:
    """Records every call routed to it. Stand-in for a real Runner."""

    def __init__(self, side_effect: ty.Callable | None = None) -> None:
        self.calls: list[tuple[ty.Callable, tuple, dict]] = []
        self._side_effect = side_effect

    def __call__(self, f: ty.Callable, args: tuple, kwargs: dict) -> ty.Any:
        self.calls.append((f, args, kwargs))
        if self._side_effect is not None:
            return self._side_effect(f, args, kwargs)
        return f(*args, **kwargs)


def test_non_recursive_function_dispatches_once():
    runner = _CountingRunner()

    @use_runner(ty.cast(Runner, runner))
    def add(x: int, y: int) -> int:
        return x + y

    assert add(2, 3) == 5
    assert len(runner.calls) == 1


def test_bypass_routes_call_through_function_body():
    runner = _CountingRunner()

    @use_runner(ty.cast(Runner, runner))
    def add(x: int, y: int) -> int:
        return x + y

    # Simulate the "we are inside the remote entry point for `add`" state.
    with unwrap_use_runner(add):
        assert add(2, 3) == 5
    assert runner.calls == []  # bypass means runner is not consulted


def test_recursive_call_re_enters_runner():
    """The canonical bug: a recursive call should re-dispatch through the runner."""
    runner = _CountingRunner()

    @use_runner(ty.cast(Runner, runner))
    def fib(n: int) -> int:
        if n <= 1:
            return n
        return fib(n - 1) + fib(n - 2)

    # Simulate the runner having dispatched `fib(3)` and now running it as the
    # "remote" side, i.e. inside an unwrap_use_runner context.
    with unwrap_use_runner(fib):
        result = fib(3)

    assert result == 2  # fib(3) = fib(2) + fib(1) = 1 + 1 = 2
    # fib(3) bypasses the runner (that's the outer one we're simulating).
    # Each of fib(2), fib(1), fib(1), fib(0) — the four recursive calls — should
    # have re-entered the runner.
    assert len(runner.calls) == 4, (
        f"Expected 4 re-entered runner calls (one per recursive sub-call), "
        f"got {len(runner.calls)}: {[c[1] for c in runner.calls]}"
    )


def test_recursive_with_memoization_avoids_redundant_runner_calls():
    """If the runner is memoizing, the runner should be called once per unique
    argument tuple — not once per recursive call. This is what `mops` actually
    provides; this test asserts the property without using mops itself."""

    cache: dict[tuple, int] = {}

    def memoizing_side_effect(f: ty.Callable, args: tuple, kwargs: dict) -> int:
        if args in cache:
            return cache[args]
        result = f(*args, **kwargs)
        cache[args] = result
        return result

    runner = _CountingRunner(side_effect=memoizing_side_effect)

    @use_runner(ty.cast(Runner, runner))
    def fib(n: int) -> int:
        if n <= 1:
            return n
        return fib(n - 1) + fib(n - 2)

    with unwrap_use_runner(fib):
        assert fib(6) == 8

    # fib(6) is bypassed; recursive calls for fib(5), fib(4), fib(3), fib(2),
    # fib(1), fib(0) each hit the runner once thanks to memoization. So we
    # expect exactly 6 runner invocations.
    unique_args = {call[1] for call in runner.calls}
    assert unique_args == {(5,), (4,), (3,), (2,), (1,), (0,)}, (
        f"Expected one runner call per unique recursive argument, got {[c[1] for c in runner.calls]}"
    )


def test_mutual_recursion_re_enters_for_both_functions():
    """Two functions that call each other should each re-enter the runner."""
    runner = _CountingRunner()

    @use_runner(ty.cast(Runner, runner))
    def is_even(n: int) -> bool:
        if n == 0:
            return True
        return is_odd(n - 1)

    @use_runner(ty.cast(Runner, runner))
    def is_odd(n: int) -> bool:
        if n == 0:
            return False
        return is_even(n - 1)

    # Simulate the outer is_even(3) being dispatched and running on the "remote."
    with unwrap_use_runner(is_even):
        result = is_even(3)

    assert result is False
    # is_even(3) bypasses. Its body calls is_odd(2) — that should re-enter the runner.
    # is_odd(2) body calls is_even(1) — re-enters. is_even(1) body calls is_odd(0) — re-enters.
    # is_odd(0) returns directly. Total: 3 re-entered calls.
    assert len(runner.calls) == 3, (
        f"Expected 3 mutual-recursion runner calls, got {len(runner.calls)}: "
        f"{[(c[0].__name__, c[1]) for c in runner.calls]}"
    )


def test_bypass_is_keyed_to_the_unwrap_target():
    """unwrap_use_runner(f) only bypasses the wrapped call for `f` itself. Calls to
    other wrapped functions don't burn the bypass — they re-enter the runner normally."""
    runner = _CountingRunner()

    @use_runner(ty.cast(Runner, runner))
    def first(x: int) -> int:
        return second(x) + 1

    @use_runner(ty.cast(Runner, runner))
    def second(x: int) -> int:
        return x * 2

    with unwrap_use_runner(first):
        # `first` consumes the bypass and runs directly; `second`, called from
        # `first`'s body, re-enters the runner normally.
        assert first(5) == 11

    assert len(runner.calls) == 1, (
        f"Expected exactly one re-entered runner call (for `second`), got "
        f"{[(c[0].__name__, c[1]) for c in runner.calls]}"
    )
    assert runner.calls[0][0].__name__ == "second"


def test_unwrap_for_a_doesnt_bypass_b_called_first():
    """If the unwrap is for `a` but a's wrapper is skipped (e.g. resolved via getattr
    to the raw function in the @pure.magic.wand pattern), the next wrapped call is for
    `b` — which should NOT bypass, because its identity doesn't match. This is the UA
    integration-test shape that the counter-based bypass broke."""
    runner = _CountingRunner()

    @use_runner(ty.cast(Runner, runner))
    def a(x: int) -> int:
        # not exercised — we deliberately don't call this through its wrapper.
        return b(x)

    @use_runner(ty.cast(Runner, runner))
    def b(x: int) -> int:
        return x + 1

    with unwrap_use_runner(a):
        # mimic "outer wrapper was skipped; body of `a` runs and calls `b`".
        # `b`'s wrapper must dispatch through the runner, NOT bypass.
        assert b(5) == 6

    assert len(runner.calls) == 1, (
        f"Expected b to dispatch through the runner, got {[(c[0].__name__, c[1]) for c in runner.calls]}"
    )
    assert runner.calls[0][0].__name__ == "b"
