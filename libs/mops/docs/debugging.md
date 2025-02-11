## `mops-inspect`

A command line tool making it a bit easier to inspect what mops is doing under the hood.

If you got an unexpected memoization, or you're wondering why you didn't get memoization, or your code
ran remotely but seems to have produced an unexpected result or error, you can debug by finding the
memoization/`invocation` URI and using the `mops-inspect` tool.

The memo URI will always be logged by `mops` at INFO level, generally with a blue background if a
memoized result was found, or with a green background if a new remote shell was launched to handle a
function invocation with no memoized result.

Paste that URI into the `mops-inspect` tool like so:

- `mops-inspect adls://thdsscratch/tmp/mops2-mpf/udsource/parquet/thds.ud.shared.parquet_from_sqlite--spark_parquet_dest/WaterNagBeach.0Z2X7n9s-VwIAfdgkWdG7ZtRrqsHv4kOJnDn2WM`

By default, this will pull the invocation, result, and/or exception from that directory. If you have a
large Path result or other embedded data in one or more pieces that you aren't interested in inspecting
or don't want to download, you can specify a URI pointing to just the `invocation`, `result`, or
`exception`.

`--embed` and `--loop` flags are available for more complex use cases. See the `--help`.
