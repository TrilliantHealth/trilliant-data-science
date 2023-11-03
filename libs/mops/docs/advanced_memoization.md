# Advanced Memoization

The default for `MemoizingPicklingRunner` is to transfer your entire
function argument context to a remote environment, which means that
memoization happens correctly and by default. Since the arguments to
your function are the actual arguments to your function, there's no
work left for you to do.

The default appproach is _strongly_ preferable, since it dramatically
reduces the possible scope for hidden impurity and therefore receiving
incorrect cached results.

Nevertheless, there are some cases where it may be impractical to
refactor your code such that the function being memoized is relatively
pure. In those cases, the `MemoizingPicklingRunner` now provides some
advanced facilities for lying to yourself and the system about the
true memoization key for your function.

## `KeyedLocalRunner`

1. The `KeyedLocalRunner` requires that you specify a Blob Storage
   Root, like the standard `MemoizingPicklingRunner`. Unlike that class,
   no Shell is required, because a thread-local shell will be used.

> NOTE: The KeyedLocalRunner is local-only! It cannot transfer execution
> to an actual remote execution environment, precisely because it will
> not have the ability to transfer all of your function arguments to
> that remote.

2. Instead of a Shell, you'll be providing a function that rewrites
   the standard `func, args, kwargs` tuple into something different
   immediately prior to constructing the memoization key. Usually
   you'll still want the returned `(func, args, kwargs)` tuple to be
   derived from the input, but it's ultimately up to you what sort of
   dark sorcery you want to perform. A standard approach might be to
   rewrite all arguments as keyword arguments but leave out certain
   keyword arguments that represent unpickleable resources.

Once the memoization key has been constructed from whatever your
function returns, things will proceed as normal on the orchestration
side.  However, when it comes time to actually call the function,
`KeyedLocalRunner` will dramatically simplify the invocation - instead
of downloading the pickled invocation and (args, kwargs) payload, it
will look up the original function and (args, kwargs) and call it as
though nothing had happened in the meantime. The result will stored
'normally', and future invocations will return the memoized result
based on the computed memoization key.

An example of this working end-to-end can be
[found here](../examples/impure.py).

### nil_args

Drop certain arguments from the memo key. This is the simplest
possible case. If you need something fancier, you may want to look at
how `nil_args` is implemented and use some of the utilities provided
to write your own keyfunc-creator.

```python
from thds.adls.defaults import env_root
from thds.mops import pure, impure

@pure.use_runner(impure.KeyedLocalRunner(env_root, keyfunc=impure.nil_args('conn')))
def run_limit_query_with_database_client(
    conn: sqlite3.Connection, tbl_name: str, limit: int = 3
) -> list:
    # the connection has no bearing on our pure function, so we can
    # use KeyedLocalRunner and `impure.nil_args` to memoize these results
    # without making alternative arrangements for the connection.
    return conn.execute(f"select * from {tbl_name} limit ?", (limit,)).fetchall()
```

### fully custom memo key generation

Write a function that matches the `pure.core.memo.keyfunc.Keyfunc`
type, and pass that as the keyfunc. Note that the returned arguments
MUST be bindable to the returned function using
`inspect.signature(func).bind`. You may have to get clever with how
you do this if you're trying to bind a _changed_ set of arguments
rather than just dropping values for some of them. The returned
function will not actually get called, so feel free to get as creative
as you need.
