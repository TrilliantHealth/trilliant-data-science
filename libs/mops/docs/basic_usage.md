# Basic usage

You'll decorate your function with `use_runner`, which requires a `PickleRunner` instance, which itself
requires a `Shell`.

## execution environment

`thds.mops` must be installed both in the local environment and the remote environment.

On the [orchestrator](./orchestrator.md), you will need to have _all_ dependencies available, or at least
everything that your orchestrator needs to be able to import using Python, as though it were going to run
the entire program locally.

On the [remote(s)](./remote.md), you only need to have `mops` and the dependencies for the specific
function that the remote will be responsible for executing. In practice, the simplest way to do this will
often be to package the entire Python application and install it on the remote, even though only a part
of it will actually be executed.

Often, a good way to do this will be to have a single Docker image or wheel that contains the full
application and is used for/installed on both the orchestrator and the remote(s).

## Transfer of control to the remote via a Shell

To use a `PickleRunner`, you need to choose a `Shell` implementation and environment. The best choice
will often be Python running in a Docker container spawned by [Kubernetes](./kubernetes.md), but as
demonstrated by unit tests, it is also possible to substitute something as simple as `subprocess.run`,
and at a core level, you just need to begin execution of a Python application with `mops` available for
import.

Once you have chosen and created your `Shell` object, you will need to wrap your function with a
`PickleRunner` that can reference that Shell.

```python
from thds.adls import defaults
from thds.mops import pure
from .k8s_shell import df_k8s_spot_11g_shell

run_on_k8s = pure.use_runner(MemoizingPicklingRunner(df_k8s_spot_11g_shell, defaults.mops_root))
# ^ a decorator that can now cause any pure function to be run remotely on K8s...

@run_on_k8s
def my_resource_intensive_function(model: pd.DataFrame, rounds: int) -> pd.DataFrame:
    ... do some stuff, return a dataframe

# now, call your function locally and it will be run remotely
df = my_resource_intensive_function(the_model_df, 15)
assert type(df) == pd.DataFrame
# `df` is your result, computed on and transferred back from the remote context
```

Now you run your application locally, and your functions will be called remotely and their results will
be returned to you. Your local process can call `use_runner`-decorated functions in threads or even
separate processes, though threads are your best bet for the large majority of scenarios.

## Development vs production

In general, you should use `thds.adls.defaults.mops_root` as lazily-loaded configuration to
`MemoizingPicklingRunner`.

You should also use `invocation_output_fqn()` with no storage_root argument in most cases when defining
an output location for large output data - the storage root will be automatically derived from the root
of your mops execution context.

However, if you're doing a production run and want to [memoize](./memoization.md) your results so that
others can use them, you should [configure](./config.md#production-runs) `mops` to use a different
Storage Account and Container for long-term storage of the invocations, results, and any output
[Paths](./paths.md) or [Source objects](./optimizations.md#thds-core-source).

### Runner bypass

This system has been carefully designed to allow you to integrate it with your existing code with a
minimum of changes. This includes the ability to directly call the functions you're integrating with
`mops` while bypassing all use of `mops`.

Because Python is a very dynamic language, you have many options for how to implement this in your own
application. For highly dynamic cases, your best bet may be to write your own application logic to
conditionally apply the decorator to functions that you do or do not want to run remotely at runtime.

However, for all-or-nothing cases, such as local test runs of a pipeline designed to be run remotely,
your application may simply pass `skip_runner=True` to the `use_runner` decorator factory wherever you
are using it, and all calls via that decorator will be directly passed to the implementing function
without further ceremony.

- `skip_runner` may also be any parameterless callable returning a `bool` in order to make this easy to
  configure at runtime, e.g. `lambda: bool(os.getenv('YOUR_APP_NO_REMOTE'))`.
