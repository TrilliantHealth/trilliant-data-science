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

## 0.3.1

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
