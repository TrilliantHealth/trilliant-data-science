"""Register large function arguments for content-addressed serialization.

When a `pure.magic` function receives a large argument (e.g. a 726K-element
tuple) on every invocation, the normal pickle path re-serializes it per
thread. `runner.shared()` content-addresses the object once and replaces it
with a ~100 byte reference in all subsequent pickles — but `pure.magic`
didn't expose this.

This module bridges the gap: given a set of argument names at decoration
time, it extracts the corresponding values at call time and registers them
via `runner.shared()`. The function signature is cached once; per-call cost
is a `bind()` + dict lookups (skipped entirely when no names are configured).
"""

import inspect
import typing as ty

if ty.TYPE_CHECKING:
    from ..pickling.mprunner import MemoizingPicklingRunner


class SharedArgRegistrar:
    """Caches the function signature at decoration time. At call time,
    extracts named args and registers them with runner.shared().
    """

    def __init__(self, func: ty.Callable, arg_names: tuple[str, ...]):
        self._sig = inspect.signature(func)
        valid_params = set(self._sig.parameters)
        bad = [n for n in arg_names if n not in valid_params]
        if bad:
            raise TypeError(
                f".shared() names {bad} do not match any parameter of {func.__qualname__}. "
                f"Valid parameters: {sorted(valid_params)}"
            )
        self._arg_names = arg_names

    def register(self, runner: "MemoizingPicklingRunner", args: tuple, kwargs: dict) -> None:
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        for name in self._arg_names:
            val = bound.arguments.get(name)
            if val is not None:
                runner.shared(**{name: val})
