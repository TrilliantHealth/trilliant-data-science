# Optimizations

Because transferring execution to a [remote](./remote.md) runtime
involves [serialization](./serialization.md), there can be a
performance penalty to using `mops`.

Therefore, `mops` also introduces optimizations to help avoid
compute resource bottlenecks, particularly on the
[orchestrator](./orchestrator.md) process.

## Large shared objects

**(Upload a large object once per pipeline)**

For large objects being passed many times as read-only parameters to
functions, you may reduce serialization and upload resource usage by
identifying those objects as 'shared'.

Under the hood, this defers serialization until the time of upload
based on the object's ID. Objects that are 'shared' must be both
pickleable and weak referenceable; noisy errors will occur if they are
not. Pandas DataFrames and Numpy NDArrays will work out of the
box. Python `list`s will not, though subclassing can wrap them to make
them weak-referenceable. Consult the Python
[`weakref` docs](https://docs.python.org/3.8/library/weakref.html) for
more details.

This is a feature of the `PickleRunner`, and usage will look something
like this:

```python
runner = AdlsPickleRunner(...)

def your_orchestrator(...):
    ...
	runner.shared(training_x_df, y=training_y_ndarr)

    ...
	the_remote_df_func(training_x_df, training_y_ndarr, ...)


@use_runner(runner)
def the_remote_df_func(x_df, y_ndarr, ...):
    ...
```

If passed as a keyword parameter, the name serves only for debugging
purposes - otherwise it is meaningless and there is no risk of
collision.

## File transfer

**(Simple large-file sources and sinks via `pathlib.Path`)

There are lots of Python objects that take up significantly more space
in memory than on disk. A parquet-ified DataFrame is a pretty common
example. Your existing parallel code may even pass local paths to
separate processes so that you can avoid transferring large amounts of
memory when it can more easily be read from disk. This makes your
functions impure, but can certainly save time and occasionally
resources.

We provide two different optimizations for files, with different use
cases. Both will dramatically reduce peak memory pressure on the
orchestrator process, because the data gets streamed from/to disk
rather than being held in memory all at once. If you have concerns
about memory pressure during the 'reduce' step of your application
(i.e. as you are collecting large quantities of results from many
remote tasks), you should consider using one of these two approaches
rather than transferring in-memory bytes objects.

 * Both abstractions require your files to be used **_either_** as a
   **immutable read-only source** file **_or_** as a **write-only,
   write-once destination**. This is _not_ a general purpose remote
   filesystem abstraction, because that would not be compatible with the
   high-level map/reduce paradigm we are attempting to provide.

 * _ADLS required for remote use_ - Remote use of these abstractions
   both depend on using the included `AdlsPickleRunner` with the
   `use_runner` decorator factory. Any future runner implementations
   will not exhibit this behavior by default.

 * _Local computation supported_ - However, both of these abstractions
   are also _usable_ with no `AdlsPickleRunner`. This means that your
   pipeline can be expressed in terms of these file abstractions
   without actually requiring ADLS or AdlsPickleRunner or K8s, etc.

### Paths

Use `pathlib.Path` and get files (but not directories) transferred
automagically.

 - _The primary use case here is one-way transfer from an orchestrator
to remote runners, with the added advantage that it requires no
modification to existing code._

Any `pathlib.Path` found (even recursively) while pickling your
function arguments will be streamed in a memory-efficient manner to
ADLS, and then on the 'other side' it will be streamed to a
**read-only** file and given to you as a `Path` object. The file is
immutable, and even if written to, those effects will not be visible
to the orchestrator.

> The file must already exist; a Path pointing to a directory or to
> nothing at all will cause subclass of ValueError to be raised.

Separately, any `Path` found in the return value will similarly be
streamed to a **read-only** file on the local orchestrator. This is
semantically meaningful as a *write-once destination* - writing to
this Path will transfer its bytes to the orchestrator as a temporary
file.

Both of these actions happen automatically across `use_runner`
function invocations using the `AdlsPickleRunner`, without further
changes to the code. And within a given `pipeline_id`, a unique set of
local bytes referenced by a `Path` will only be transferred up to
remote workers a single time.

Your Python code simply deals with these Paths like
normal. **However**, if you want `Path`s transferred back to an
orchestrator to live in a particular directory, you'll have to write
the code on the orchestrator side to make sure the files get moved to
the appropriate destination, since the `Path` crossing the
`use_runner` function boundary back into the orchestrator process
will point to a temporary location.

See `tests.integration.remote.shell_test.func_with_paths` to see
examples of both of these usages in action.

#### Path must exist and be a regular file!

Currently, these `Path` optimizations do not handle directories, nor
can they 'represent' a destination Path (where something should be
placed by the callee) - they may only be regular files. If you need to
upload a directory full of files, you can construct a List of Paths
yourselves and they will each be individually transferred. However,
this is an implementation limitation that could be lifted in the
future if it would be advantageous.

### Src/DestFiles

See [Src/DestFiles](./src_dest_files.md) - this is not a pure
optimization, since its interface will bleed through into the remote
function itself.
