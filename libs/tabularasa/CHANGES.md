### 0.12.2

- Further simplification of sqlite bulk inserts. WAL mode is found to have little effect on performance
  in this context, and mutates the database, so it has been removed to reduce complexity.

### 0.12.1

- The main sqlite data population routine, invokable via the `tabularasa init-sqlite` command or
  `thds.tabularasa.data_dependencies.sqlite.populate_sqlite_db` function, now acquires a file lock on the
  target database file to prevent concurrent writes, which could arise e.g. when performing
  initializations inside worker processes in a multiprocessing context.

## 0.12.0

- in `tabularasa push`, switched `no_fail_if_absent` from a positional arg parsed to boolean to a flag
  (`--no-fail-if-absent`)
- imbued `--no-fail-if-absent` with semantics in the `taburasa sync-blob-store --down`/`tabularasa pull`
  case (previously it only appled to the `--up`/`push` case). Now, if you attempt to sync a blob down
  which has no corresponding blob in the blob store, there is no error.
- in `tabularasa datagen`, the blob store sync down to local files is run with the new
  `no_fail_if_absent` semantics. Any blobs which fail to sync, that are required in the build DAG, will
  just be regenerated as part of the build, due to absence or hash mismatches.

### 0.11.3

flake8 and black

### 0.11.2

- upper bound on `pandera` version due to changes with `pandas` dependencies in that release

### 0.11.1

- internally uses `thds.core.git.get_repo_root()`

### 0.11.0

- `tabularasa pull` and `tabularasa sync-blob-store` now both accept optional `--tables` to allow for
  syncing only a subset of tables, and use multithreading for parallel I/O operations.

### 0.10.1

- Fix bug in use of `numpy.dtypes`, which does not exist in numpy\<1.25.

## 0.10.0

- `requires-python>=3.10`.
- Take upper-bounds off of `pandas`, `pandera`, and `numpy`.
  - Increases `pandas` lower-bound to `>=1.5`.
  - Increases `pandera` lower-bound to `>=0.20`.
- Renames `loaders.util.unique` to `loaders.util.unique_across_columns` and registers this as a custom
  `pandas` check in `pandera`.
- No longer coerces numeric numpy index dtypes to 64-bit when using `pandas>=2.0`.
- Enforces the use of nanosecond datetime precision across pyarrow and (numpy-based) pandas datetime
  types for consistent behavior.
  - `pandas<2.0` only supported nanosecond precision, so sticking with that for for simplicity across
    `pandas` versions.
- Adds a `compat` module with some code that gets rendered (to be called) on `numpy` numeric types set as
  indices in `pandas.DataFrame`s.

### 0.9.4

- Added tests for `FileSourceMixin.needs_update`.
- Added a new valid value, `Biannual`, for a table's `update_frequency`.

### 0.9.3

- Removed some annoying module-level CLI-related warnings from the main module, and improved the error
  message when required CLI dependencies are not installed.

### 0.9.2

- `data-diff` now errors when passed table names which are not in the schema.
- `data-diff` now shows metadata changes for diff-able tables.

### 0.9.1

- Migrating from `poetry` -> `uv` for project management + some typing fixes.

## 0.9.0

- Added a new `--value-detail` flag and `--value-detail-min-count` option to the `data-diff` command to
  allow for inspecting counts of specific row-level updates for each column that was modified in a table.

## 0.8.0

- Made the `datagen` command perform a data sync by default to avoid errors arising from stale data, with
  a new optional `--no-sync` flag to disable this behavior.
- Added a new `--debug` flag to the `data-diff` command to drop into a debugger whenever a positive table
  diff is found.

### 0.7.3

- Fixed a bug in the use of `ruamel.yaml` in the CLI which caused errors when multithreading `tabularasa`
  schema loads/saves in parallel.

### 0.7.2

- Fixed the use of buggy pandas index intersection method

### 0.7.1

- `tabularasa data-diff` updates:
  - Optimization: skips computing diffs for tables whose historical and current md5 hashes are equal
  - Table diffs print in alphabetical order by table name
  - `tabulate`'s `floatfmt` arg is used explicitly and set to `.6g` to avoid excessive significant digits

## 0.7.0

- Fixed broken rendering of URLs in sphinx documentation as produced by `tabularasa docgen`
- Added github urls for those source data files in a schema that are in version control

## 0.6.0

- Added new `schema-diff` command to compare the current schema against a previous version of the schema
  and output the differences in a human-readable markdown format.

## 0.5.0

- `tabularasa datagen` now ensures _all_ tables in the schema are recomputed when run without args

## 0.4.0

- Changed metaschema and semantics for `inherit_schema` to facilitate simpler reasoning about the schema
  of tables using inheritance:
  - `exclude_columns` is now forbidden
  - `columns` is added and when specified lists a set of column names to explicitly inherit from the
    parent tables

### 0.3.7

- Adding `sqlite_from_parquet` for conversion from parquet to a DB defined by a tabularasa schema

### 0.3.6

- Fixes for mypy 1.11

### 0.3.5

- Set some reasonable defaults in schema `build_options`, specifically:
  - `type_constraint_comments` is now `True` by default
  - `validate_transient_tables` is now `True` by default
  - `require_typing_extensions` is now `False` by default since python 3.7 is at end-of-life

### 0.3.4

- Lazily load some sqlite databases from the somewhat more flexible `core.Source`, rather than the
  ADLS-specific `adls.AdlsHashedResource`.

### 0.3.3

- Added logic in Schema to inform when a data source needs to be checked for updates.

### 0.3.2

- Added a `read_only` flag to the `sqlite_connection` routine, `False` by default but set to `True` in
  `AttrsSQLiteDatabase`. This prevents errors when multiple threads share a connection in read-only
  workflows.

### 0.3.1

- More verbose logging of sqlite db path when populating db.

## 0.3.0

- Added a helper function `check_categorical_dtype` to use consistently on all reads and writes when
  dealing with `pandas.CategoricalDtype`s, to raise loud errors consistently everywhere when unexpected
  values are present rather than allow pandas' usual behavior of nulling those values out on cast.

### 0.2.1

- Added helper function that adds line breaks to long strings. The function is used when formatting
  Sphinx tables.

## 0.2

- Migrating to Python ^3.9 (numpy/pandas stack no longer supports 3.8).

## 0.1

- Initial release in the monorepo.
