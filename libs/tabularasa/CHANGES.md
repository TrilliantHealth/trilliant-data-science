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
