# thds.mops.remote

A facility for transparently calling Python functions in a remote
process from a local orchestrator process by passing the arguments and
the results to and from the remote process via some kind of Python
object channel.

The current `AdlsPickleRunner` implementation uses:

- `pickle` for serialization
- `ADLS` for remote storage

though each of these implementation details is independent of the
others, and other implementations of the same system could be written.

The standard remote shell implementation is Kubernetes, but integration
tests run using a simple subprocess shell, and running on Azure
Functions or local Docker containers would be easy to implement.

## Philosophy (important!)

The overarching goal of this is to realize the potential for
_parallelizing_
[**_functions_**](https://en.wikipedia.org/wiki/Pure_function) using
simple and scalable architectural primitives. A function which depends
on nothing but its input variable(s) and produces no meaningful result
other than a returned value may be easily transposed into a different
runtime context, and is therefore easily parallelized. This is the
principle behind `multiprocess.Pool`, behind the `MapReduce` paradigm,
and many other implementations of the same idea.

What this means for the user of this system is that you're going to
have an easy time if your computation is already encapsulated in a
pure function, and a progressively harder time (more work to be done)
the less [pure](https://en.wikipedia.org/wiki/Pure_function) your
existing computation is. You will almost certainly need to refactor
out any existing instances of impurity.

Practically speaking, some things to avoid/remove from your functions:
 * Global constants or mutable variables.
 * Direct use of environment variables.
 * Modification of input parameters.
 * Files or other state external to the function (with exceptions for
   certain narrow usages of filesystem primitives as described in
   detail below)
 * References to large amounts of static reference data. If
   possible, select only the data you need before passing it to the
   function. If not, see the section on
   [named shared objects](#named-shared-objects)
 * Not returning a result that contains everything you might possibly
   want to know about your computation.

By implementating truly pure functions, we can keep the **What** (the
business logic of your function) separate from the **How** (the
details of what environment it runs on), which not only enables
plug-and-play parallelization but also makes your code much easier to
read and reason about. In the end analysis, if your functions are
_truly_ pure, you won't even need `mops` in the long run -
you'll be able to find other off-the-shelf libraries and frameworks
that will let you parallelize your computation.

This library does provide some benefits that other frameworks will
not, and attempts to make it possible to convert lots of disparate
codebases to use its facilities without invasive changes. In order to
remain relatively simple, however, it requires that you piece together
various pieces and understand how those pieces fit together. Read on
for specifics...

## Usage

`thds.mops` must be installed both in the local environment and
the remote environment, both of which should provide the full Python
application that is to be split across local and remote.

To use the `AdlsPickleRunner`, you need to choose a `Shell`
implementation and environment. The best choice will usually be a
Docker container hosted by Kubernetes, but as demonstrated by unit
tests, it is also possible to substitute something as simple as
`subprocess.run`.

If using K8s, you will need to construct a `k8s_shell` with
configuration appropriate to the Python function that will ultimately
be invoked remotely. This includes specifying the name of your Docker
container as well as providing resource requests or limitations. For example:

```python
import thds.mops.k8s as mops_k8s

df_k8s_spot_11g_shell = mops_k8s.k8s_shell(
    mops_k8s.autocr('ds/demand-forecast:latest'),
    # gotta specify which container this runs on
    node_narrowing=dict(
        resource_requests={"cpu": "3.6", "memory": "11G"},
        tolerations=[mops_k8s.tolerates_spot()],
    ),
)
```

This will require your Docker image to exist and to be accessible to
Kubernetes via the configured Container Registry. Additionally, that
image must somehow be provided with read and write access to ADLS,
since all data will be transferred back and forth from your local
environment via ADLS. All Data Science K8s clusters will provide
read/write access to a set of ADLS Storage Accounts by default.

Once you have contructed an appropriate Shell, you need only pass it
to the `AdlsPickleRunner` constructor, and then use `pure_remote` plus
that runner to construct a decorator that will transmit computation
for any pure function to the remote K8s environment, and inject its
result right back into the local/orchestrator context.

```python
from thds.mops.remote import pure_remote, AdlsPickleRunner

run_on_k8s = pure_remote(AdlsPickleRunner(df_k8s_spot_11g_shell))
# ^ a decorator that can now cause any pure function to be run remotely on K8s...

@run_on_k8s
def my_resource_intensive_function(model: pd.DataFrame, rounds: int) -> pd.DataFrame:
    ... do some stuff, return a dataframe

# now, call your function locally and it will be run remotely
df = my_resource_intensive_function(the_model_df, 15)
assert type(df) == pd.DataFrame
# `df` is your result, computed on and transferred back from the remote context
```

That's actually it. You just run your application locally, and your
functions will be called remotely and their results will be returned
to you. Your local process can call `pure_remote`-decorated functions
in threads or even separate processes, though threads are your best
bet for the large majority of scenarios.

By default, `mops` has chosen certain ADLS Storage
Accounts and Containers to be used for data transfer. These may be
customized via `AdlsPickleRunner` parameters, but you should probably
stick with the defaults in most cases.

### Remote runner bypass

This system has been carefully designed to allow you to integrate with
it with a minimum of code changes, specifically including the ability
to call the same functions in a local-only environment with no code
changes.

Because Python is a very dynamic language, you have many options for
how to implement this in your own application. For highly dynamic
cases, your best bet may be to write your own application logic to
conditionally apply the decorator to functions that you do or do not
want to run remotely at runtime.

However, for all-or-nothing cases, such as local test runs of a
pipeline designed to be run remotely, your application may simply pass
`bypass_remote=True` to the `pure_remote` decorator factory wherever
you are using it, and all calls via that decorator will be directly
passed to the implementing function without further
ceremony.
* `bypass_remote` may also be any parameterless callable returning a
  `bool` in order to make this easy to configure at runtime,
  e.g. `lambda: bool(os.getenv('YOUR_APP_NO_REMOTE'))`.

## Concurrency for `pure_remote` functions

When running many `pure_remote` functions concurrently, you should
generally prefer to use threads rather than parallel processes. Shared
memory within the orchestration parts of the process allows for much
simpler reuse of various contexts, and since the Runner you are using
almost certainly builds in process-level parallelism, there's no
additional advantage (and _many_ possible disadvantages) to
layering extra polling processes on top of the underlying processes.

To help with this, we've provided
`remote.parallel.parallel_yield_results`, which should be general
enough for many use cases. If it is not, feel free to bring your own
concurrency primitives.

### Joblib backend

A simple `joblib` backend is also provided, for cases where you might
already be using it, or if the library you're using
(e.g. `scikit-learn`) is already using it under the hood.

Please note that running thousands of very short (e.g. ten second)
tasks is not something K8S excels at...

## Named shared objects

**(Upload a large object once per pipeline)**

For large objects being passed many times as read-only parameters to
functions, you may reduce serialization and upload resource usage by
'naming' those objects.

Under the hood, this defers serialization until the time of upload
based on the object's ID. Objects that are 'named' must be both
pickleable and weak referenceable; noisy errors will occur if they are
not. Pandas DataFrames and Numpy NDArrays will work out of the
box. Python `list`s will not, though subclassing can wrap them to make
them weak-referenceable. Consult the Python
[`weakref` docs](https://docs.python.org/3.8/library/weakref.html) for
more details.

The name itself does not need to correspond to any other usage within
your program; it serves as a global-to-the-pipeline unique,
consistent identifier inside ADLS and the pickles that depend on it,
and must therefore be unique to your pipeline.

This is a feature of the `AdlsPickleRunner`, and usage will look
something like this:

```python
runner = AdlsPickleRunner(...)

def your_orchestrator(...):
    ...
	runner.named(x=training_x_df, y=training_y_ndarr)

    ...
	the_k8s_func(training_x_df, training_y_ndarr, ...)


@pure_remote(runner)
def the_k8s_func(x_df, y_ndarr, ...):
    ...
```

## File transfer

**(Many-byte sources and sinks via `pathlib.Path` and
`SrcFile`/`DestFile`)**

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
   **read-only source** file **_or_** as a **write-only destination**. This is
   _not_ a general purpose remote filesystem abstraction, because that
   would not be compatible with the high-level map/reduce paradigm we
   are attempting to provide.

 * _ADLS required for remote use_ - Remote use of these abstractions
   both depend on using the included `AdlsPickleRunner` with the
   `pure_remote` decorator factory. Any future runner implementations
   will not exhibit this behavior by default.

 * _Local computation supported_ - However, both of these abstractions
   are also _usable_ with no `AdlsPickleRunner`. This means that your
   pipeline can be expressed in terms of these file abstractions
   without actually requiring ADLS or AdlsPickleRunner or K8s, etc.

### streaming pathlib.Path transfer

 - _The primary use case here is one-way transfer from an orchestrator
to remote runners, with the added advantage that it requires no
modification to existing code._

Any `pathlib.Path` found (even recursively) while pickling your
function arguments will be streamed in a memory-efficient manner to
ADLS, and then on the 'other side' it will be streamed to a temporary
file and given to you as a `Path` object. It will be semantically
meaningful as a *read-only source* - writing to this file will produce
no effect on the orchestrator process.

Separately, any `Path` found in the result will similarly be streamed
to a temporary file on the local orchestrator. This is semantically
meaningful as a *write-only destination* - writing to this Path will
transfer its bytes to the orchestrator as a temporary file.

Both of these actions happen automatically across `pure_remote`
function invocations using the `AdlsPickleRunner`, without further
changes to the code. And within a given `pipeline_id`, a unique `Path`
found locally will only be transferred up to remote workers a single
time.

Your Python code simply deals with these Paths like
normal. **However**, if you want `Path`s transferred back to an
orchestrator to live in a particular directory, you'll have to write
the code on the orchestrator side to make sure the files get moved to
the appropriate destination, since the `Path` crossing the
`pure_remote` function boundary back into the orchestrator process
will point to a temporary location.

See `tests.integration.remote.shell_test.func_with_paths` to see
examples of both of these usages in action.

#### Path must exist and be a regular file!

Currently, these `Path` optimizations do not handle directories, nor
can they 'represent' a destination Path (where something should be
placed by the callee) - they may only be regular files. However, this
is an implementation limitation that could be lifted in the future if
it would be advantageous.

### Src/Dest Files

 - _The primary use case here is large amounts of data being generated
remotely, and you want to be able to point future processes and/or
remote runtimes at that data._

 - ADLS is the only supported remote filesystem for these two
   abstractions.

The `DestFile` is defined on the local orchestrator, specifying (via a
Context Manager) a remote path at which a remote function should place
a file result of its computation. Upon return to the local
orchestrator process, a small, JSON, "remote file pointer" will be
placed on local storage at the local path.

* You must pass the `DestFile` to the remote function **and** _return_
  it back from the function.

The populated `DestFile` can then be 'converted' on the local
orchestrator (either from the remote pointer on the local filesystem,
or the `DestFile` object already in memory) into a `SrcFile` and
passed directly to future remote functions, where, when accessed via
its Context Manager, the backing remote file will be downloaded and
made available for read-only access.

Both of these abstractions are specifically designed to allow you to
use them even if you're not running remotely - they will transparently
use the local filesystem instead of ADLS with no performance penalty.

As an example of this flow, you might do something like the following:

```python
from thds.mops.remote import DestFile, SrcFile, adls_dataset_context, pure_remote, ...

def orchestrator(...):
    my_ds = adls_dataset_context('my-dataset')
    created_dest = remote_creator(my_ds.dest('relative/path/where/i/want/it.parquet'))
    # created_dest now actually exists on your filesystem, but only as a pointer
    result_dest = remote_processor(
        my_ds.dest('relative/path/to/final/result.parquet'),
        my_ds.src(created_dest),
    )
    # result_dest also exists on your filesystem as a pointer.

@pure_remote(...)
def remote_creator(dest: DestFile, *args, **kwargs) -> DestFile:
    created_file_path = create_stuff(*args, **kwargs)
    with dest as dest_path:
        # when this context closes, the file at dest_path will be uploaded as necessary
        created_file_path.rename(dest_path)
        return dest  # dest must be returned in order to be referenced in the orchestrator

@pure_remote(...)
def remote_processor(dest: DestFile, src: SrcFile, *args, **kwargs) -> DestFile:
    with src as src_path:
        # this makes sure the src path is available locally
        result_df = process_stuff(src_path, *args, **kwargs)
        with dest as dest_path:
            result_df.to_parquet(dest_path)
            return dest_path
```

The Context Managers are a bit ugly, so you may wish to use
`core.scope.enter` to avoid all of the nesting.

⚠️ However, it is critical that your `DestFile` context be closed
_before_ exiting the `pure_remote`-decorated function, or else your
data will remain in the temp location and will not get delivered to
its final destination.

`SrcFile` actually supports three different methods of creation,
depending on your situation.

1. Locally present file that you want uploaded for a given process run
   if and only if the function actually gets run remotely. Use
   `AdlsDatasetContext.src`. A local-only run will transparently use
   the local file.
2. Locally present remote file pointer (JSON string), created using
   `DestFile` or with some other means. Use `AdlsDatasetContext.src`
   for this as well. A local-only run will have to download the file
   inside the function where it is used.
3. Fully remote file that you have never downloaded, with no
   locally-present remote file pointer. Use
   `AdlsDatasetContext.remote_src`. A local-only run will have to
   download the file.

Any use of a remote-only `SrcFile` will require that it be downloaded
 upon every access, even if computing locally (skipping the
 `pure_remote` decorator). At the present time, there is no fancy
 caching that happens on a per-process basis. The `SrcFile` Context
 Manager will be forced to re-download the file after every `__exit__`
 and subsequent `__enter__`. This is an implementation detail and
 could potentially be lifted in the future.

`DestFile` supports two methods of creation.

1. On the local orchestrator process, before passing to the
   remote. Use `AdlsDatasetContext.dest`, which will specify a
   location for the remote file pointer to be placed upon return from
   the remote process. Prefer this usage.
2. On the remote process (inside the decorated function). Use
   `AdlsDatasetContext.remote_dest`. The data will be uploaded to that
   ADLS path, and if returned to the local orchestrator, the local
   filepath will be a concatenation of the process working directory
   and the full ADLS path.

## Result caching

Each `pure_remote` function using the AdlsPickleRunner will combine
the current `pipeline_id` plus the hash of the pickled remote
invocation, (the function and all its arguments) to determine an ADLS
path for the invocation and the results.  _Before_ invoking the
function remotely, it will check to see if a result already exists at
the expected path, and if it does, that result will be returned
directly.

This allows partial pipeline runs to have their results reused.

The most likely scenario where this might be useful would be in the
case of some kind of failure of your local orchestrator process.

**By default**, a new pipeline id is generated in every orchestrator
  process, so results will not get reused by default. Your
  orchestrator must intentionally set the pipeline id to a previously
  generated or chosen one in order to take advantage of this
  capability.

## Limitations

### No 'remote recursion'

Currently, only a single 'level' of remote functions are
supported. That is to say, once running remotely, no function which is
itself a remote function may be called and expected to be launched on
yet another remote machine.

This is a simple technical limitation and may be removed with some
additional work.

### Network bottlenecks

This library has improved over time, but over many runs we have
observed that while you can run small-to-medium workloads against ADLS
just fine, large workloads (tens of thousands of `pure_remote` calls
simultaneously) may be difficult to manage over a network connection
between your laptop and the Azure datacenters, causing spurious
authentication, connection timeout, and other network errors. If you
start running into these issues you can running your orchestrator
process on the 'inside' of an Azure datacenter on a pod inside our
Kubernetes cluster.

Until we have better documentation on this, it's recommended that you
look at the `demand-forecast` repository, where a `k8s` directory has
some utilities handy for giving yourself a temporary pod inside the
East2 Kubernetes cluster.
