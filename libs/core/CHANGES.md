### 1.45.20250815

- Adds `thread_debug.py` which can be used to debug what thread you're in and what thread(s) might have
  spawned you.

## 1.45

- Adds `core.progress.report_still_alive` decorator for long-running functions. If your function is still
  executing after N seconds, will log a message saying "still working on that". This is particularly
  useful for preventing log-read timeouts of k8s pods.
- Adds `core.iterators.titrate`. This is particularly useful for avoiding a stampede of elephants in a
  concurrent Pool Executor.

## 1.44

- `core.futures.PFuture` and `core.futures.LazyFuture` are provided to make it possible to provide
  Future-like systems across subprocesses.

## 1.43

- `refcount.Resource` is a new utility that may be useful.

## 1.42

- Adds `thds.core.fp` with `compose` for right-to-left unary fn composition and `pipe` for left-to-right.

## 1.41

- Add `find_project_root` function.

## 1.40

- Support non-stdlib hashing functions being registered for file hashing, and use this capability within
  `core.Source` for any `from_file`-constructed `Source` objects.

## 1.39

- Adds helpers to `core.inspect` for dynamically getting arguments by name, internally using
  `inspect.signature(func).find(*args, **kwargs)`.

## 1.38

- Remove dependencies on `attrs` and `cattrs`, which were lightly used anyway.
- Remove the bulk of the code having to do with reading and writing a `meta.json` file, which we have not
  used for a while. Technically this is a backward incompatible change, but since it required no
  downstream changes I think it's safe enough to pretend that it wasn't.

## 1.37

- `core.scope` now provides a parallel API for AsyncContextManagers, i.e. scopes with `__aenter__` and
  `__aexit__`.

### 1.36

- Adds `core.git.get_repo_root() -> Path`, which returns an absolute Path to the repo-root on your local
  machine

### 1.35.20250416

- Adds util function to `cpus` that returns `4` if `"CI" in os.environ`, and otherwise returns
  `available_cpu_count()`

## 1.35

- Remove patch version from core.

### 1.34.20250415

- Fixes a bug in `timer.Timer` where `secs_per_call` was erroneously being divided by 60, and ensured
  that `calls` was incremented when using the context manager method for time tracking.

### 1.34.20250408

- Fixes bug in `core.source.SourceTree` when `SourceTree.sources` contains only one `Source` - in that
  event, we want the single file's parent directory, not the file itself as the logical root.
- Adds `source.tree_from_directory(dirpath: str | os.PathLike) -> SourceTree` function.

## 1.34

- `requires-python>=3.9`.

## 1.33

- `calgitver.calgitver()` is now `lru_cached`. Which is to say, it is no longer possible to have an
  application get two different results for this; so don't change your git repo while your application is
  running.

## 1.32

- Add `source.tree.SourceTree` dataclass and the related `logical_root` utility, in order to be able to
  represent potentially remote directories deterministically.

### 1.31.20250124

- Print a traceback in `core.parallel.yield_results`, along with the type of the Exception, so that we're
  no longer mystified about what's happening during a very large/long run.

## 1.31

- Adds `scaling` module, which is a way for projects to coordinate on scaling factors using explicit
  stack-local string names, without (over)specifying a shared semantic for what those scaling
  requirements might be.

## 1.30

- Log to a file by default. This will be helpful on developer laptops, but will be especially helpful for
  some deployed use cases where we may need to have configurable log redirection for various reasons.

### 1.29.20241122

- Add support for more callable types in `scope.bound` - hopefully this will help with some obscure
  errors we've seen logged in the past.

## 1.29

- Added `default` to `dict_utils.merge_dict` that allows the user to define other values than just `None`
  when a dictionary does not have a key.
- Added `TimeTracker` to `timer`.

## 1.28

- Make `lazy.Lazy` friendlier, in that it will raise a message suggesting you add parentheses prior to
  your attribute access.

### 1.27.20241021

- Fixes bug in `core.sqlite.upsert.mappings` stemming from the fact that SQLite apparently never reaches
  the `ON CONFLICT DO UPDATE SET...` clause if the row you're trying to upsert would not be an insertable
  row on its own. In other words, if you have a `NOT NULL` constraint on a given column, and you don't
  provide a non-null value for that column because you already know the row exists and you simply want to
  update it, SQLite will raise an `IntegrityError` immediately upon seeing your row, rather than
  attempting the insert, discovering a `UNIQUE` constraint conflict, and applying the `ON CONFLICT`
  clause (as you might otherwise have expected/hoped).

## 1.27

- Add `ThdsJsonFormatter` and `ThdsLogfmter` (which is based on
  <https://github.com/jteppinette/python-logfmter/tree/main>) so that we can have more parsable and
  queryable logs in production. Use `THDS_CORE_LOG_FORMAT` to configure.

## 1.26.20240905

- Fixed bug in `core.parallel.yield_all`, which would run a single-item list twice, rather than only
  running it in the current thread, as intended.

## 1.26

- `core.config` now infers a parser for your `ConfigItem` from the type of its default value, if
  possible. In other words, if your default value is a `Path` object, you do not need to explicitly set
  `parse=Path`, because we can infer that, if the type of your default argument will idempotently 'parse'
  your default value, then it is probably a reasonable approach to trying to parse/coerce any future
  value passed in from the environment or external system.
- Fix bug in design of `core.config`, where we always ran your parser (if any) when using `set_global`,
  which might have been confusing if you had a more complex type for your config.

## 1.25

- Better `__repr__` for `source.Source` and `thunks.Thunk`.

## 1.24

- Added some additional `git` functions: `get_merge_base` and `get_commit_datetime`.

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
