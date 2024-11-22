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

import atexit
import contextlib
import inspect
import sys
import typing as ty
from functools import wraps
from logging import getLogger
from uuid import uuid4

from .inspect import get_caller_info
from .stack_context import StackContext

_KEYED_SCOPE_CONTEXTS: ty.Dict[str, StackContext[contextlib.ExitStack]] = dict()
# all non-nil ExitStacks will be closed at application exit


def _close_root_scopes_atexit():
    for name, scope_sc in _KEYED_SCOPE_CONTEXTS.items():
        scope = scope_sc()
        if scope:
            try:
                scope.close()
            except ValueError as ve:
                print(f"Unable to close scope '{name}' at exit because {ve}", file=sys.stderr)


atexit.register(_close_root_scopes_atexit)


def _init_sc(key: str, val: contextlib.ExitStack):
    """This should only ever be called at the root of/during import of a
    module. It is _not_ threadsafe.
    """
    # normally you shouldn't create a StackContext except as a
    # global.  in this case, we're dynamically storing _in_ a
    # global dict, which is equivalent.
    if key in _KEYED_SCOPE_CONTEXTS:
        getLogger(__name__).warning(
            f"Scope {key} already exists! If this is not importlib.reload, you have a problem."
        )
    _KEYED_SCOPE_CONTEXTS[key] = StackContext(key, val)


F = ty.TypeVar("F", bound=ty.Callable)


def _bound(key: str, func: F) -> F:
    """A decorator that establishes a scope boundary for context managers
    that can now be `enter`ed, and will then be exited when this
    boundary is returned to.
    """
    if inspect.isgeneratorfunction(func):

        @wraps(func)
        def __scope_boundary_generator_wrap(*args, **kwargs):
            if key not in _KEYED_SCOPE_CONTEXTS:
                _init_sc(key, contextlib.ExitStack())  # this root stack will probably not get used

            with _KEYED_SCOPE_CONTEXTS[key].set(contextlib.ExitStack()) as scoped_exit_stack:
                with scoped_exit_stack:  # enter and exit the ExitStack itself
                    ret = yield from func(*args, **kwargs)
                    return ret  # weird syntax here, Python...

        return ty.cast(F, __scope_boundary_generator_wrap)

    if inspect.isasyncgenfunction(func):

        @wraps(func)
        async def __scope_boundary_async_generator_wrap(*args, **kwargs):
            if key not in _KEYED_SCOPE_CONTEXTS:
                _init_sc(key, contextlib.ExitStack())

            with _KEYED_SCOPE_CONTEXTS[key].set(contextlib.ExitStack()) as scoped_exit_stack:
                with scoped_exit_stack:
                    async for ret in func(*args, **kwargs):
                        yield ret

        return ty.cast(F, __scope_boundary_async_generator_wrap)

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def __scope_boundary_coroutine_wrap(*args, **kwargs):
            if key not in _KEYED_SCOPE_CONTEXTS:
                _init_sc(key, contextlib.ExitStack())

            with _KEYED_SCOPE_CONTEXTS[key].set(contextlib.ExitStack()) as scoped_exit_stack:
                with scoped_exit_stack:
                    return await func(*args, **kwargs)

        return ty.cast(F, __scope_boundary_coroutine_wrap)

    @wraps(func)
    def __scope_boundary_wrap(*args, **kwargs):
        if key not in _KEYED_SCOPE_CONTEXTS:
            _init_sc(key, contextlib.ExitStack())  # this root stack will probably not get used

        with _KEYED_SCOPE_CONTEXTS[key].set(contextlib.ExitStack()) as scoped_exit_stack:
            with scoped_exit_stack:  # enter and exit the ExitStack itself
                return func(*args, **kwargs)

    return ty.cast(F, __scope_boundary_wrap)


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
    raise NoScopeFound(f"No scope with the key {key} was found.")


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
        _init_sc(self.key, contextlib.ExitStack())  # add root boundary

    def bound(self, func: F) -> F:
        """Add a boundary to this function which will close all of the
        Contexts subsequently entered at the time this function exits.
        """
        return _bound(self.key, func)

    def enter(self, context: ty.ContextManager[M]) -> M:
        """Enter the provided Context with a future exit at the nearest boundary for this Scope."""
        return _enter(self.key, context)


default = Scope("__default_scope_stack")
bound = default.bound
enter = default.enter
