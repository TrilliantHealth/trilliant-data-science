## 1.7

- Adds new `params` submodule with utilities for inspecting and parameterizing generic types. These can
  be used in conjunction with `TypeRecursion` to implement custom handling of generic record types.
- Adds general support for `dataclasses`-defined record types in `TypeRecursion`.

## 1.6

- Adds an `empty` submodule with a utility for generating empty instances of arbitrary types.
- Adds a new `ConstructorFactory` class derived from `TypeRecursion` but with greater type-safety by
  constraining the recursion to return only callables that construct instances of the input type.
  - This is now used in both `attrs_utils.empty` and `attrs_utils.random.gen`, the latter of which has
    improved type safety as a result.

## 1.5

- Adds a `cattrs.errors` submodule with custom exceptions and extensible utilities for pretty-printing
  cattrs structuring errors.
- Changes exception type of restricted type conversions from plain `TypeError` to a custom
  `DisallowedConversionError` to allow for custom handling.

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
