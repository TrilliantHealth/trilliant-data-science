## 1.8.20230822

- Fix downstream usage of `get_env` to be `active_env` as originally
  intended.

## 1.8

- Can configure the level of any Trilliant Health logger by name
  (usually its module name) via newline-separated entries in a text
  file pointed to by the environment variable `THDS_LOGLEVELS`.

### 1.7.20230803

- Added a `py4j` logging filter relevant to running on Databricks Runtime 11.3.

## 1.7

- `fretry` module for in-house retry decorators.

## 1.6

- Add `env` module that just defines a known set of application-global
  'environments' that other libraries and applications can use for
  whatever purposes.

## 1.5.20230707

- Some name format regex clean up in `meta`.

## 1.5

- Added the inverse function, `unflatten`, of `flatten` in `dict_utils`.

## 1.4

- New `Lazy` and `ThreadLocalLazy` implementation, both promoted from
  `thds.mops`.

## 1.3

- Added a `dict_utils` module, providing useful functions and classes for dictionaries.

## 1.2

- Add `hashing` utility.

### 1.1.20230504030030

- Reworking `thds.core.meta` cattrs (un)structure hooks to work with Python > 3.8.
- Some bug and consistency fixes in `thds.core.meta`.
- Further building out test suite.
