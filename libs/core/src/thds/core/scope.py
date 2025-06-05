"""This allows the usage of ContextManagers that cover the entire body
of a function without requiring invasive (and git-diff-increasing)
"with" statements.

Another way of looking at this is that it is essentially a
decorator-driven `defer` in Go or `scope` in D. However, the semantics
are slightly different in that the scope is not necessarily at the
nearest function call boundary - we use Python's dynamic capabilities
to look 'up' the stack until we find a scope that is usable, and then
we embed the ContextManager in that scope.

Generally, the usage will look something like this:

```
@scope.bound  # wrap a function with a scope that will exit when it returns
def do_stuff(...):
    foo = scope.enter(a_context_manager(...))  # enters the context manager via the nearest scope
    # ...do some stuff
    return bar
    # context manager exits when nearest scope exits, which is right after function return.
```

where the traditional alternative would be:

```
def do_stuff(...):
    with a_context_manager(...) as foo:
        # ...do the same stuff
        # ...but now your git diff is huge b/c you indented everything,
        return bar
    # context manager exits after `with` block closes, which is when the function returns.
```

Because we use ContextVar to perform the lookups of the nearest scope,
this is actually pretty performant.  You do pay the cost of the
wrapping function, which is higher than a `with` statement.

"""

import asyncio
import atexit
import contextlib
import inspect
import sys
import typing as ty
from collections import defaultdict
from functools import wraps
from logging import getLogger
from uuid import uuid4

from .inspect import get_caller_info
from .stack_context import StackContext

K = ty.TypeVar("K")
V = ty.TypeVar("V")


class _keydefaultdict(defaultdict[K, V]):
    def __init__(self, default_factory: ty.Callable[[K], V], *args, **kwargs):
        super().__init__(default_factory, *args, **kwargs)  # type: ignore
        self.key_default_factory = default_factory

    def __missing__(self, key: K) -> V:
        ret = self[key] = self.key_default_factory(key)
        return ret


_KEYED_SCOPE_CONTEXTS: dict[str, StackContext[contextlib.ExitStack]] = _keydefaultdict(
    lambda key: StackContext(key, contextlib.ExitStack())
)
_KEYED_SCOPE_ASYNC_CONTEXTS: dict[str, StackContext[contextlib.AsyncExitStack]] = _keydefaultdict(
    lambda key: StackContext(key, contextlib.AsyncExitStack())
)
# all non-nil ExitStacks will be closed at application exit


def _close_root_scopes_atexit():
    for name, scope_sc in _KEYED_SCOPE_CONTEXTS.items():
        exit_stack_cm = scope_sc()
        if exit_stack_cm:
            try:
                exit_stack_cm.__exit__(None, None, None)
            except Exception as exc:
                print(f"Unable to exit scope '{name}' at exit because {exc}", file=sys.stderr)

    async def do_async_cleanup():
        for name, scope_sc in _KEYED_SCOPE_ASYNC_CONTEXTS.items():
            exit_stack_cm = scope_sc()
            if exit_stack_cm:
                try:
                    await exit_stack_cm.__aexit__(None, None, None)
                except Exception as exc:
                    print(f"Unable to exit async scope '{name}' at exit because {exc}", file=sys.stderr)

    if _KEYED_SCOPE_ASYNC_CONTEXTS:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(do_async_cleanup())
        finally:
            loop.close()


atexit.register(_close_root_scopes_atexit)


F = ty.TypeVar("F", bound=ty.Callable)


def _bound(key: str, func: F) -> F:
    """A decorator that establishes a scope boundary for context managers
    that can now be `enter`ed, and will then be exited when this
    boundary is returned to.
    """
    if inspect.isgeneratorfunction(func):

        @wraps(func)
        def __scope_boundary_generator_wrap(*args, **kwargs):
            with _KEYED_SCOPE_CONTEXTS[key].set(contextlib.ExitStack()) as scoped_exit_stack:
                with scoped_exit_stack:  # enter and exit the ExitStack itself
                    ret = yield from func(*args, **kwargs)
                    return ret  # weird syntax here, Python...

        return ty.cast(F, __scope_boundary_generator_wrap)

    if inspect.isasyncgenfunction(func):

        @wraps(func)
        async def __scope_boundary_async_generator_wrap(*args, **kwargs):
            with _KEYED_SCOPE_CONTEXTS[key].set(contextlib.ExitStack()) as scoped_exit_stack:
                with scoped_exit_stack:
                    async for ret in func(*args, **kwargs):
                        yield ret

        return ty.cast(F, __scope_boundary_async_generator_wrap)

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def __scope_boundary_coroutine_wrap(*args, **kwargs):
            with _KEYED_SCOPE_CONTEXTS[key].set(contextlib.ExitStack()) as scoped_exit_stack:
                with scoped_exit_stack:
                    return await func(*args, **kwargs)

        return ty.cast(F, __scope_boundary_coroutine_wrap)

    @wraps(func)
    def __scope_boundary_wrap(*args, **kwargs):
        with _KEYED_SCOPE_CONTEXTS[key].set(contextlib.ExitStack()) as scoped_exit_stack:
            with scoped_exit_stack:  # enter and exit the ExitStack itself
                return func(*args, **kwargs)

    return ty.cast(F, __scope_boundary_wrap)


def _async_bound(key: str, func: F) -> F:
    """A decorator that establishes a scope boundary for context managers
    that can now be `aenter`ed, and will then be aexited when this
    boundary is returned to.
    """
    if inspect.isasyncgenfunction(func):

        @wraps(func)
        async def __scope_boundary_async_generator_wrap(*args, **kwargs):
            with _KEYED_SCOPE_ASYNC_CONTEXTS[key].set(contextlib.AsyncExitStack()) as scoped_exit_stack:
                async with scoped_exit_stack:
                    async for ret in func(*args, **kwargs):
                        yield ret

        return ty.cast(F, __scope_boundary_async_generator_wrap)

    assert inspect.iscoroutinefunction(func), "You should not use async_bound on non-async functions."

    @wraps(func)
    async def __scope_boundary_coroutine_wrap(*args, **kwargs):
        with _KEYED_SCOPE_ASYNC_CONTEXTS[key].set(contextlib.AsyncExitStack()) as scoped_exit_stack:
            async with scoped_exit_stack:
                return await func(*args, **kwargs)

    return ty.cast(F, __scope_boundary_coroutine_wrap)


class NoScopeFound(Exception):
    pass


M = ty.TypeVar("M")


def _enter(key: str, context: ty.ContextManager[M]) -> M:
    """Call this to enter a ContextManager which will be exited at the
    nearest scope boundary, without needing a with statement.
    """
    # this is fairly efficient - we don't walk up the stack; we simply
    # use the stack-following ContextVar that was set up previously.
    scope_context = _KEYED_SCOPE_CONTEXTS.get(key)
    if scope_context:
        return scope_context().enter_context(context)
    raise NoScopeFound(f"No scope with the key {key} was found - did you call .bound()?")


async def _async_enter(key: str, context: ty.AsyncContextManager[M]) -> M:
    scope_context = _KEYED_SCOPE_ASYNC_CONTEXTS.get(key)
    if scope_context:
        return await scope_context().enter_async_context(context)
    raise NoScopeFound(f"No async scope with the key {key} was found - did you call .async_bound()?")


class Scope:
    """Creating your own Scope isn't often necessary - often you just want
    a basic scope around your function, so you can just use the default Scope,
    which is created below.

    However, in case it's important to your use case to be able to
    have orthogonal scopes that can be entered further down the stack
    and exited at a precise point further up, this makes it possible.

    If you provide a key, it must be globally unique, and if it has
    previously been created within the same application, an
    AssertionError will be thrown. You do not need to provide a key.

    These should be module-level/global objects under all
    circumstances, as they share an internal global namespace.

    """

    def __init__(self, key: str = ""):
        caller_info = get_caller_info(skip=1)
        self.key = caller_info.module + "+" + (key or uuid4().hex)
        if self.key in _KEYED_SCOPE_CONTEXTS:
            getLogger(__name__).warning(
                f"Scope with key '{self.key}' already exists! If this is not importlib.reload, you have a problem."
            )
        else:
            _KEYED_SCOPE_CONTEXTS[self.key] = StackContext(self.key, contextlib.ExitStack())

    def bound(self, func: F) -> F:
        """Add a boundary to this function which will close all of the
        Contexts subsequently entered at the time this function exits.
        """
        return _bound(self.key, func)

    def enter(self, context: ty.ContextManager[M]) -> M:
        """Enter the provided Context with a future exit at the nearest boundary for this Scope."""
        return _enter(self.key, context)


class AsyncScope:
    """See docs for Scope - but this is the one you use when you have async context
    managers.

    These should be module-level/global objects under all
    circumstances, as they share an internal global namespace.

    """

    def __init__(self, key: str = ""):
        caller_info = get_caller_info(skip=1)
        self.key = caller_info.module + "+" + (key or uuid4().hex)
        if self.key in _KEYED_SCOPE_ASYNC_CONTEXTS:
            getLogger(__name__).warning(
                f"Async scope with key '{self.key}' already exists! If this is not importlib.reload, you have a problem."
            )
        else:
            _KEYED_SCOPE_ASYNC_CONTEXTS[self.key] = StackContext(self.key, contextlib.AsyncExitStack())

    def async_bound(self, func: F) -> F:
        """Add an async context management boundary to this function - it will _only_
        use async context managers, i.e. __aenter__ and __aexit__ methods.

        You can wrap an async function with a synchronous bound and use synchronous
        context managers with it, but there's no point to wrapping your sync function
        with an async_bound because you won't be able to enter the context within it.
        """
        return _async_bound(self.key, func)

    async def async_enter(self, context: ty.AsyncContextManager[M]) -> M:
        """Enter the provided Context with a future exit at the nearest boundary for this Scope."""
        return await _async_enter(self.key, context)


default = Scope("__default_scope_stack")
bound = default.bound
enter = default.enter
default_async = AsyncScope("__default_async_scope_stack")
async_bound = default_async.async_bound
async_enter = default_async.async_enter
