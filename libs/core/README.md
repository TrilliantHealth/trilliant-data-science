# core Library

The monorepo successor to `core`

## Development

If making changes to the library please add an entry to `CHANGES.md`, and if the change is more than a patch,
please bump the version in `pyproject.toml` accordingly.


## Logging config

This library handles configuration of all DS loggers. By default, all
INFO-and-above messages are written (to `stderr`). To customize what
level different modules are logged at, you should create a file that
looks like this:


```
[debug]
thds.adls.download
...
[warning]
thds.mops.remote.pickle_runner
...
```

You may also add an `*` to change the global default log level, e.g.:

```
...
[warning]
*
```

Provide the path to this file to `thds.core` via the `THDS_LOGLEVELS`
environment variable. You may wish to create this file and then set
its path via exported envvar in your `.bash/zshrc` so that you can
permanently tune our logging to meet your preferences.
