# Advanced Memoization

The default for `MemoizingPicklingRunner` is to transfer your wrapped computation context to a remote
environment, which means that memoization happens correctly and by default. Since the arguments to your
wrapped computation are converted directly into a memoization key, there's no work left for you to do.

The default approach is _strongly_ preferable, since it dramatically reduces the possible scope for
hidden impurity and therefore receiving incorrect cached results.

Nevertheless, there are some cases where it may be impractical to refactor your code such that the
computation being memoized is relatively pure. In those cases, the `MemoizingPicklingRunner` now provides
some advanced facilities for lying to yourself and the system about the true memoization key for your
computation.

## `KeyedLocalRunner`

1. The `KeyedLocalRunner` requires that you specify a Blob Storage Root, just like the standard
   `MemoizingPicklingRunner`.

> NOTE: The KeyedLocalRunner is local-only! It cannot transfer execution to an actual remote execution
> environment, precisely because it will not have the ability to transfer all of your function arguments
> to that remote.

2. The second required argument is a `keyfunc` function, to which the original computation
   `(c, args, kwargs)` will be passed immediately prior to constructing the memoization key. Its purpose
   is to return a modified tuple of those `(c, args, kwargs)`\[^1\] that will be used to construct the
   memoization key - usually you'll still want the modified tuple to be derived from the originals, but
   it's ultimately up to you what sort of dark sorcery you want to perform. A standard approach might be
   to rewrite all arguments as keyword arguments but leave out certain keyword arguments that represent
   unpickleable resources.

\[^1\]: Why do you need to return the callable? Why do we pass it to your keyfunc in the first
place?\[^2\] Because we build the memo key off your function name, but _you_ might want to substitute in
a different name for the memo key than the actual wrapped computation has. But in most cases you probably
will just return the callable directly without modifying it.

\[^2\]: You actually don't need to receive the callable or pass it back if you don't want to. If your
keyfunc's first argument is not named `c`, we will not pass you the callable at all, and we'll assume you
didn't want its name in the memo key to change. Instead, we'll splat `args` and `kwargs` into your
keyfunc.

> The returned `(c, args, kwargs)` tuple will NOT be used when finally executing a computation that has
> no pre-existing memoized result. The original wrapped computation will be called directly with the
> original arguments and keyword arguments.

An example of this working end-to-end can be [found here](../examples/impure.py).

### nil_args

Drop certain arguments from the memo key. This is the simplest possible case. If you need something
fancier, you may want to look at how `nil_args` is implemented and use some of the utilities provided to
write your own keyfunc-creator.

```python
from thds.adls.defaults import mops_root
from thds.mops import pure, impure

@pure.use_runner(impure.KeyedLocalRunner(mops_root, keyfunc=impure.nil_args('conn')))
def run_limit_query_with_database_client(
    conn: sqlite3.Connection, tbl_name: str, limit: int = 3
) -> list:
    # the connection has no bearing on our pure function, so we can
    # use KeyedLocalRunner and `impure.nil_args` to memoize these results
    # without making alternative arrangements for the connection.
    return conn.execute(f"select * from {tbl_name} limit ?", (limit,)).fetchall()
```

### fully custom memo key generation

Write a function that matches the `pure.core.memo.keyfunc.Keyfunc` type, and pass that as the keyfunc.
Note that the returned arguments MUST be bindable to the returned function using
`inspect.signature(func).bind`. You may have to get clever with how you do this if you're trying to bind
a _changed_ set of arguments rather than just dropping values for some of them. The returned function
will not actually get called, so feel free to get as creative as you need.
