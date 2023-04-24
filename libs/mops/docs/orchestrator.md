# What is an orchestrator?

Simply put, it's a process that tells [other processes](./remote.md)
what to do. Often, it then collects results from those processes and
organizes them, or sends those results to other processes for another
round of orchestrated task completion.

A good internal example of an orchestrator is
[`demand-forecast`](https://github.com/TrilliantHealth/demand-forecast),
which orchestrates the extraction of various slices of Unified Asset
data, then orchestrates the training of various models on that data,
and then orchestrates prediction/forecast based on those models.

Pyspark code is usually a form of orchestration, though it acts at a
somewhat higher level than `mops`, since your Spark code will often go
through a query planner of some sort before the underlying tasks are
generated. `mops` takes a lower-level approach that assumes you
already have known tasks that you want to execute, and that you're
willing to write your own code to 'plan' or 'order' their execution.

## Rules for orchestrators

### Concurrency

When running many `pure_remote` functions concurrently, you should
prefer to use **threads** rather than parallel processes. Shared
memory within the orchestration parts of the process allows for much
simpler reuse of various contexts, and since the Runner you are using
almost certainly builds in process-level parallelism, there's no
additional advantage (and _many_ possible disadvantages) to layering
extra polling processes on top of the underlying processes.

To help with this, we've provided
`thds.mops.remote.parallel:parallel_yield_results`, which should be
general enough for many use cases. If it is not, feel free to bring
your own concurrency primitives.

#### Joblib backend

A simple `joblib` backend is also provided, for cases where you might
already be using it, or if the library you're using
(e.g. `scikit-learn`) is already using it under the hood.

Please note that running thousands of very short (e.g. ten second)
tasks is not something K8S excels at...
