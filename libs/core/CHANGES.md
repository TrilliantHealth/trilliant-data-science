## 1.23

- Adds an optional `custom_merger` parameter to `sqlite.sqlmap.parallel_to_sqlite`, which defaults to
  `sqlite.merge.merge_databases` with all of its defaults partially applied.
- `sqlite.sqlmap.merge_sqlite_dirs` now requires a `merger` parameter.
- These `[custom_]merger` parameters are typed as `ty.Callable[[ty.Iterable[types.StrOrPath]], Path]`.

## 1.22

- Adds a `cache` module with a `locking` decorator that ensures only one invocation of a cached-wrapped
  function runs for each `*args, **kwargs` key.

## 1.21

- Add `atomic_binary_writer` helper context manager, for writing file output with a higher level of
  concurrency safety.

## 1.20

- Added `concurrency.named_lock`.

## 1.19

- Added `tmp.tempdir_same_fs`.

## 1.18

- `link.cheap_copy` affords a way to make an atomic copy of a file with known permissions.

### 1.17.20240209

- Remove remaining race conditions in `core.link:link` - there was a simple solution all along, which is
  to create a temporary link at a random file on the same filesystem as the true destination, and then
  perform `os.rename` on the link!

## 1.17

- New `tmp` module with utilities for getting a temporary Path on the same filesystem as a 'destination'
  path, so that atomic moves from the temp location to your destination are possible.
- Fix bug in `fretry` with interaction between `retry_sleep` and `expo`, which was getting consumed once
  and never performing retries after that.

## 1.16

- New `Source` abstraction for read-only data which may or may not yet be local. Try it with
  `source.from_path` and `source.from_uri`.

## 1.15

- Updated `DotDict` to convert `dict`s inside of `set`s, `list`s, and `tuple`s into `DotDict`s.

  ```python
  dd = DotDict({"letters": [{"a": 1}, {"b": 2}]})
  dd.letters[0].a == 1
  dd.letters[1].b == 2
  ```

## 1.14

- `meta.get_user` no longer logs at the `WARNING` level about pulling the system user as this is "normal"
  behavior in many circumstances. It still logs a `DEBUG` statement.

## 1.13

- Changed the behavior of `DotDict.get_value`. Now, if the path does not exist in the `DotDict` we will
  return `None` instead of raising a `KeyError`.

  ```python
  dd = DotDict({"a": 1, "b": {"c": 2}})
  dd.get_value("b.c") == 2
  dd.get_value("z") == None
  ```

## 1.12

- 'Promote' `exit-after` script from `mops` to `core`, as `thds-exit-after`.

## 1.11

- 'Promote' `link` from the `thds.adls` library as it has no ADLS-specific functionality.

## 1.10

- Add `config` system to `core` that enables a broad range of functionality that can be non-invasively
  integrated into existing applications as desired.

## 1.9

- Added the beginnings of `concurrency.py`, which will be a collection of utilities for sprinkling
  concurrency into our systems with a slightly more opinionated approach.
- `env.temp_env` now has a default of `""` which will result in the current active env being set for the
  duration of the `env.temp_env` context manager.

### 1.8.20230907

- Fix an unfortunate issue where generator functions were not getting wrapped correctly by `scope.bound`,
  leading to the scope never actually getting closed.

## 1.8.20230822

- Fix downstream usage of `get_env` to be `active_env` as originally intended.

## 1.8

- Can configure the level of any Trilliant Health logger by name (usually its module name) via
  newline-separated entries in a text file pointed to by the environment variable `THDS_LOGLEVELS`.

### 1.7.20230803

- Added a `py4j` logging filter relevant to running on Databricks Runtime 11.3.

## 1.7

- `fretry` module for in-house retry decorators.

## 1.6

- Add `env` module that just defines a known set of application-global 'environments' that other
  libraries and applications can use for whatever purposes.

## 1.5.20230707

- Some name format regex clean up in `meta`.

## 1.5

- Added the inverse function, `unflatten`, of `flatten` in `dict_utils`.

## 1.4

- New `Lazy` and `ThreadLocalLazy` implementation, both promoted from `thds.mops`.

## 1.3

- Added a `dict_utils` module, providing useful functions and classes for dictionaries.

## 1.2

- Add `hashing` utility.

### 1.1.20230504030030

- Reworking `thds.core.meta` cattrs (un)structure hooks to work with Python > 3.8.
- Some bug and consistency fixes in `thds.core.meta`.
- Further building out test suite.
