# Configuring `mops`

`mops` ships with a limited set of default config that is generally
suitable for data science at Trilliant Health. This includes basic
information about where our [Kubernetes](./kubernetes.md) cluster is
(should you need to use that), some default timeouts, and the ADLS
storage account and container that should be used by default for two
different scenarios.

This configuration file is [TOML](https://toml.io/en/), and if you
wish to configure `mops` differently, there are _several_
[different](../src/thds/mops/config.py) approaches that you can take,
depending on which is most convenient for your use case.

## standalone TOML config

Create a `mops`-only TOML config file, and make sure it includes, at a
minimum, values for the keys defined in the default
[`thds.mops.east_config.toml`](../src/thds/mops/east_config.toml).

### MOPS_CONFIG

Take that file, name it whatever you want, and set the environment
variable `MOPS_CONFIG` to point to its fully-qualified path.

### .mops.toml

Or, name that file `.mops.toml` and place that in one of the following locations:

1. The current working directory for the Python process that imports
   `mops`.
2. Your home directory.

## combined config

Or, define and load the config however you want (e.g. YAML, JSON, XML)
within your project, and once you have a nested Python dictionary that
corresponds to the multilevel config table that `mops` expects, call
`thds.mops.config:set_global_config` before beginning to use `mops`.

## dynamic stack-local config

`mops` will also allow your Python application to dynamically override
particular config values on a per-thread/async coroutine level. It
provides a context manager for configuration that will allow you to
set the config for all code called 'below' your `with` statement, like
so:

```python

with mops.config.set_config("mops", "memo", "storage_root")(your_value):
    call_some_func()
	do_more_stuff()
	# everything in here will see your config value,
	# while anything else running in the same process
	# will see whatever the previously-configured value is.
	...
use_original_config()  # and this will not see the configured value from above
```

# Production runs

The default `mops` configuration file ships with
"development-appropriate defaults." The most critical of these to
change if you are doing a production run where you want the results to
be available forever and in a sensible location will be:

`mops.memo.storage_root` - change this so that your hash-addressed
memoized invocations and results will not be deleted after 30
days. You should probably set it to `adls://thdsdatasets/prod-datasets`.

`mops.datasets.storage_root` - change this so any `DestFiles` created
(or `SrcFiles` uploaded) will not be deleted after 30 days. This also
should likely be set to `adls://thdsdatasets/prod-datasets`, but you may
wish to consult with others who have used `mops` in the past or senior
members of the team who know where data ought to live.
