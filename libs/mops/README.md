# `mops`

`mops` is a Python library that solves two problems that compose:

- **Running** Python functions on different runtimes (Kubernetes, Databricks) without leaving Python.
- **Memoizing** their results in shared blob storage so they don't have to be recomputed.

Most people arrive at `mops` for the first problem — "I need to run this on something bigger than my
laptop." Memoization comes along for the ride and often turns out to be the more valuable half once you
see what it enables: crashed pipelines restart without redoing work, results compute once and are reused
by anyone with access to the same blob store, and downstream code can simply call upstream functions to
consume precomputed results — no separate provenance system needed.

You can use either independently, but most people end up using both.

## `mops` is a library, not a service

When you call a `mops`-decorated function, your **local Python process** does all the coordination. It
hashes the arguments, checks shared blob storage for an existing result, and — if none exists — submits
work to whichever runtime you chose (e.g. creates a Kubernetes Job using your local credentials). That
Job's entrypoint _is_ `mops` - a one-shot invocation that reads your pickled call from blob storage, runs
your function, writes the result back, and exits. Your local process picks up the result and returns it
to the caller.

There is no `mops` service waiting on either side. `mops` is just installed in both Python environments
(your local machine and the remote runtime), and the blob store is the only thing they communicate
through. The remote runtime is whatever you already use to run jobs - Kubernetes, Databricks, etc. - not
a `mops`-managed deployment.

```
   user code         mops (local)        blob store         kubernetes
   =========         ============        ==========         ==========
        |                  |                  |                  |
        | result = f(x)    |                  |                  |
        |----------------->|                  |                  |
        |                  | check memo URI   |                  |
        |                  |----------------->|                  |
        |                  |  (miss)          |                  |
        |                  | write invocation |                  |
        |                  |----------------->|                  |
        |                  | create Job       |                  |
        |                  |--------------------------------->   |
        |                  |                  | read invocation  |
        |                  |                  |<-----------------|
        |                  |                  |   run f(x)       |
        |                  |                  |   on pod         |
        |                  |                  | write result     |
        |                  |                  |<-----------------|
        |                  | watcher: done    |                  |
        |                  |<-----------------------------------|
        |                  | read result      |                  |
        |                  |----------------->|                  |
        |                  | (deserialize)    |                  |
        |   return value   |                  |                  |
        |<-----------------|                  |                  |
        | use result       |                  |                  |
```

The user code (top lane) doesn't know about any of the middle steps — it's just a function call that
returns a value. Every step between is `mops` coordinating local + blob store + remote runtime.

## What gets cached, and how to control it

`mops` computes a deterministic storage location (the **memo URI**) from:

- the **blob root** (configurable: e.g. `adls://example-storage/cache` or `file://~/.mops`),
- a fixed `mops2-mpf` segment (not configurable; mops uses this to namespace its own storage),
- the **pipeline id** (a grouping mechanism — see [pipeline-ids](docs/pipeline-ids.adoc)),
- the **fully-qualified function name** (module + name — _not_ the function's code),
- and a **SHA-256 hash of the function arguments**.

```
adls://example-storage/cache/mops2-mpf/my-pipeline/my_pkg.transform:run/abc123.../
^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^
blob root                    (fixed)   pipeline id module:function_name args hash
```

The function's **code** is never hashed. To force a fresh result, the dominant pattern is to pass a
different output URI as an explicit function argument; `mops`'s cache key is derived from all function
arguments, so changing the URI changes the memo URI and forces a fresh run. `pipeline_id` exists for
higher-level grouping that rarely changes. For ways to mark a function's logic as "changed" without
renaming it, see [function-logic-key](docs/memoization.adoc#configurability).

See [memoization](docs/memoization.adoc) for the full breakdown.

## Where to go next

Jump to [Quickstart](docs/quickstart.adoc) if you ~~are impatient~~ prefer examples, like me!

`mops` is used by [decorating or wrapping your pure function and then calling it](docs/magic.adoc) like a
normal function.

## Requirements

- Python >= 3.10
- `thds.mops` installed in both the [local/orchestrator](docs/orchestrator.adoc) environment and the
  [remote](docs/remote.adoc) execution context.
- Your code is structured as ['pure' functions](docs/pure_functions.adoc).
- Function and arguments are [serializable](docs/serialization.adoc) with `pickle`.
- (If using remote compute) ADLS read+write access on both sides.

It is usually used with remote compute on:

- [Kubernetes](docs/kubernetes.adoc), with code distributed as a Docker image
- or [dbxtend](../dbxtend/README.md) (currently internal-only), with code distributed to Databricks as
  Python wheels.

It optionally integrates with:

- [`joblib`](https://joblib.readthedocs.io/en/latest) for local parallelism.

## Limitations

It has some [limitations](docs/limitations.adoc).

## Tools & Debugging

- [CLI tools](docs/tools.adoc) for inspecting and summarizing mops invocations
- [Debugging guide](docs/debugging.adoc) - storage structure, metadata files, run IDs, and diagnosing
  race conditions

## Development

If making changes to the library, please bump the version in `pyproject.toml` accordingly.

Also look at our [changelog](CHANGES.md).

### Running tests

- `uv run pytest tests --test-uri-root file://./mops-tests`
- `uv run pytest tests -m integration --test-uri-root file://./mops-tests`

If you want to run tests against a non-bundled blob store, you will need to make sure that blob store is
installed in the venv before running the tests.
