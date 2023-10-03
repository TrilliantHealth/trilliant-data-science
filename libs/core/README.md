# core Library

The monorepo successor to `core`

## Development

If making changes to the library please add an entry to `CHANGES.md`, and if the change is more than a patch,
please bump the version in `pyproject.toml` accordingly.


## Logging config

This library handles configuration of all DS loggers. By default, all
INFO-and-above messages are written (to `stderr`).

### File format

To customize what level different modules are logged at, you should
create a file that looks like this:


```
[debug]
thds.adls.download
thds.adls.link
[warning]
thds.mops.pure.pickle_runner
thds.mops.k8s.watch
```

You may also/instead add an `*` to change the global default log level, e.g.:

```
[warning]
*
```

> The wildcard syntax is not a generic pattern-matching facility;
> it _only_ matches the root logger.
>
> However, if you wish to match a subtree of the logger hierarchy,
> this is built in with Python loggers; simply configure `thds.adls`
> under `[debug]` and all otherwise-unconfigured loggers under
> `thds.adls` will now log at the DEBUG level.

### `THDS_LOGLEVELS` environment variable

Provide the path to the above-formatted file to `thds.core` via the
`THDS_LOGLEVELS` environment variable. You may wish to create this
file and then set its path via exported envvar in your `.bash/zshrc`
so that you can permanently tune our logging to meet your preferences.
