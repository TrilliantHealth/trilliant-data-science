# `mops`

`mops` is a Python library for ML Operations.

[See the user docs here](docs/README.md).

Also look at our [changelog](./CHANGES.md).

## Tools

- `mops-inspect` - a tool for inspecting the `(args, kwargs)` of a given `mops` function invocation. If
  you're not getting the memoization you expect, this is a good place to start. Pass the full memo URI,
  e.g. `adls://thdsscratch/tmp/mops2-mpf/....`

- `mops-summarize` - summarize an entire application run with multiple `mops` function invocations. See
  the full [README here](src/thds/mops/pure/tools/summarize/README.md).

## Development

If making changes to the library, please bump the version in `pyproject.toml` accordingly.
