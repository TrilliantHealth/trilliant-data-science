## 1.4

- Requires `python>=3.9` now.

## 1.3

- Added a `try_bases` helper for compositional use with `TypeRecursion` to enable a fallback method for
  type resolution via inherited types when the other recursion paths fail.

## 1.2

- `attrs_value_defaults` helper function for extracting single-valued defaults from an `attrs` class.

## 1.1

- `NamedTuple.__field_types` was deprecated in favor of `NamedTuple.__annotations__` in Python 3.9.
  Changing references in the former in this library to the latter. `__annotations__` is available in all
  currently supported version of Python.

# 1.0

- Initial release in the monorepo.
