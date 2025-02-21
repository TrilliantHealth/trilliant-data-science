# Non-optimized large-file sources and sinks via `pathlib.Path` (deprecated)

**Use `pathlib.Path` and get files (but not directories) transferred automagically.**

- _The primary use case here is one-way transfer from an orchestrator to remote runners, with the added
  advantage that it requires no modification to existing code._

Any `pathlib.Path` found (even recursively) while pickling your function arguments will be streamed in a
memory-efficient manner to ADLS, and then on the 'other side' it will be streamed to a **read-only** file
and given to you as a `Path` object. The file is immutable, and even if written to, those effects will
not be visible to the orchestrator.

> The file must already exist; a Path pointing to a directory or to nothing at all will cause subclass of
> ValueError to be raised.

Separately, any `Path` found in the return value will similarly be streamed to a **read-only** file on
the local orchestrator. This is semantically meaningful as a _write-once destination_ - writing to this
Path will transfer its bytes to the orchestrator as a temporary file.

Both of these actions happen automatically across `use_runner` function invocations using the
`MemoizingPicklingRunner`, without further changes to the code. And within a given `pipeline_id`, a
unique set of local bytes referenced by a `Path` will only be transferred up to remote workers a single
time.

Your Python code simply deals with these Paths like normal. **However**, if you want `Path`s transferred
back to an orchestrator to live in a particular directory, you'll have to write the code on the
orchestrator side to make sure the files get moved to the appropriate destination, since the `Path`
crossing the `use_runner` function boundary back into the orchestrator process will point to a temporary
location.

See `tests.integration.remote.shell_test.func_with_paths` to see examples of both of these usages in
action.

### Path must exist and be a regular file!

Currently, these `Path` optimizations do not handle directories, nor can they 'represent' a destination
Path (where something should be placed by the callee) - they may only be regular files. If you need to
upload a directory full of files, you can construct a List of Paths yourselves and they will each be
individually transferred. However, this is an implementation limitation that could be lifted in the
future if it would be advantageous.

### Automatic, Forced Downloading

Paths will be force-downloaded before control returns to the application. This is in many cases
inefficient, as an orchestrator may only wish to hand off the Path to another consuming function which
will run remotely. This is a very good reason to prefer using the `thds.core.source` abstraction, which
will avoid forced downloads of data in environments that do not actually consume it.
