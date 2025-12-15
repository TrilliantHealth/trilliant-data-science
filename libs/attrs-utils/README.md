# `thds.attrs-utils` Library

This library contains utilities for working with basic data types and type annotations in a generic way -
transforming, checking, generating, or anything else you'd want to do with types.

## Supported types:

- Builtin types, e.g. `int`, `str`, `float`, `bool`, `bytes`
- `datetime.date`, `datetime.datetime`
- Most standard library collection types, e.g. `List[T]`, `Sequence[T]`, `Dict[K, V]`, `Mapping[K, V]`,
  `Set[T]`
- Heterogeneous tuple types, e.g. `typing.Tuple[A, B, C]`
- Variadic tuple types, e.g. `Tuple[T, ...]`
- `typing.NamedTuple` record types
- `attrs`-defined record types, including generics with type variables
- `dataclasses`-defined record types, including generics with type variables
- Union types using `typing.Union`
- `typing.Literal`
- `typing.Annotated`
- `typing.NewType`

## General Recursion Framework

The `thds.attrs_utils.type_recursion` module defines a generic interface for performing operations on
arbitrarily nested types. If you have some operation you'd like to do, e.g. transform a python data model
into some other schema language, or define a generic validation check, all you need to do is define it on
a particular set of cases.

For example, here's a simple implementation that counts the number of types referenced inside of a nested
type definition:

```python
from typing import List, Mapping, Tuple, get_args
from thds.attrs_utils.type_recursion import TypeRecursion, Registry

def n_types_generic(recurse, type_):
    args = get_args(type_)
    return 1 + sum(map(recurse, args))

n_types = TypeRecursion(
    Registry(),
    tuple=n_types_generic,       # these aren't strictly required because the implementation is the same for all of them
    collection=n_types_generic,  # but I include them
    mapping=n_types_generic,
    otherwise=n_types_generic,
)

print(n_types(Mapping[Tuple[int, str], List[bytes]]))
#             1       2     3    4     5    6
# 6
```

This example is very simple to illustrate the point. However, much more complex use cases are enabled by
the framework. Most useful are type recursions which accept types and return _callables_ that apply to or
return values inhabiting those types. Examples included in this library are

- an instance checker takes an arbitrarily nested type and returns a callable which recursively checks
  that all fields inside a nested value are of the expected type
- a jsonschema generator which takes a type and returns a jsonschema, which can then be used to validate
  deserialized values that may be structured into instances of that type
- a random generator which takes a type and returns random instances of that type

Note that the cases which return callables are _static_ with respect to the given type. This allows you
to freeze the callable as specialized to a specific type, so that the type itself only has to be
inspected only once - the callable itself only needs to inspect values.

## Use Cases in this Library

This library includes a few useful implementations of the above pattern.

### Random Data Generation

You can create a callable to generate instances of a given type as follows:

```python
import itertools
from typing import Dict, Generic, Literal, NewType, Optional, Tuple, TypeVar
import attr
from thds.attrs_utils.random.builtin import random_bool_gen, random_int_gen, random_str_gen
from thds.attrs_utils.random.tuple import random_tuple_gen
from thds.attrs_utils.random.attrs import register_random_gen_by_field
from thds.attrs_utils.random import random_gen

@register_random_gen_by_field(
    a=random_str_gen(random_int_gen(1, 3), "ABCD"),
    b=random_tuple_gen(random_int_gen(0, 3), random_bool_gen(0.99))
)
@attr.define
class Record1:
    a: Optional[str]
    b: Tuple[int, bool]

ID = TypeVar("ID")
Key = Literal["foo", "bar", "baz"]

@attr.define
class Record2(Generic[ID]):
    id: ID
    records: Dict[Key, Record1]

MyID = NewType("MyID", int)

ids = itertools.count(1)
random_gen.register(MyID, lambda: next(ids))

random_record = random_gen(Record2[MyID])

print(random_record())
print(random_record())
# Record2(id=1, records={'bar': Record1(a='B', b=(1, True)), 'baz': Record1(a='C', b=(3, True)), 'foo': Record1(a='ACB', b=(1, True))})
# Record2(id=2, records={'foo': Record1(a='A', b=(3, True)), 'bar': Record1(a='ADB', b=(1, True)), 'baz': Record1(a='CAD', b=(0, True))})
```

This can be useful for certain kinds of tests, e.g. round-trip tests, run-time profiling, and
property-based tests. It saves you maintenance because you don't need a sample of "real" data that is
completely up to date with your data model changes, and it saves you time because it's faster to generate
random instances in memory than to fetch a file and deserialize instances from it.

### Validation

There are two kinds of validation provided in this library: jsonschema validation and basic instance
checking.

#### Jsonschema

Jsonschema validation applies to an "unstructured" precursor of your data that would come, e.g. from
parsing json or deserializing data in some other way. This expects a value composed of builtin python
types - dicts, lists, strings, ints, floats, bools, and null values, arbitrarily nested.

To generate a jsonschema for your type (usually a nested record type of some kind), you need only run the
following:

```python
from thds.attrs_utils.jsonschema import to_jsonschema, jsonschema_validator

from my_library import my_module

schema = to_jsonschema(my_module.MyRecordType, modules=[my_module])

check = jsonschema_validator(schema)

check({})  # fails for absence of fields defined in my_module.MyRecordType
```

#### Simple instance checks

Instance checking asserts that the run time types of all references inside some object are as expected.
It is semantically similar to the builtin `isinstance`, but checks all references inside an object
recursively.

```python
from typing import Literal, Mapping

from thds.attrs_utils.isinstance import isinstance as deep_isinstance

Num = Literal["one", "two", "three"]

value = {"one": 2, "three": 4}

# can't use `isinstance` with parameterized types
print(isinstance(value, Mapping))
# True
print(deep_isinstance(value, Mapping[Num, int]))
# True
print(deep_isinstance(value, Mapping[str, int]))
# True
print(deep_isinstance(value, Mapping[Num, str]))
# False
```

This can be useful for validating data from an unknown source, but is generally less useful that
jsonschema validation, because it applies to data that has already been "structured", (assuming that the
input was even in the correct shape for such an operation), and most of the errors it would catch could
also be caught statically and more efficiently via static type checking. We provide it mainly as a
reference implementation for using the `TypeRecursion` framework in a relatively simple, but mostly
complete way. We also use it in a property-based test of random data generation; for any type `T`,
`isinstance(random_gen(T)(), T)` should hold.

## Serialization/Deserialization

The `thds.attrs_utils.cattrs` submodule defines useful defaults for serialization/deserialization of
values of various types, and utils to customize behavior for your own custom types, should you need to.
The goal is that the defaults do what you want in 99% of cases.

To use the converters:

```python
from thds.attrs_utils.cattrs import DEFAULT_JSON_CONVERTER

from my_library import my_module

ready_for_json = DEFAULT_JSON_CONVERTER.unstructure(my_module.MyRecordType())
```

or if you require some custom behavior, you may define your own hooks and use helper functions to
construct your own converter. Here's an example where we register custom hooks for the UUID type, which
you would need if that type was present in your data model:

```python
from typing import Type
from uuid import UUID

from thds.attrs_utils.cattrs import default_converter, setup_converter, DEFAULT_STRUCTURE_HOOKS, DEFAULT_UNSTRUCTURE_HOOKS_JSON

def structure_uuid(s: str, type_: Type[UUID]) -> UUID:
  return type_(s)


CONVERTER = setup_converter(
    default_converter(),
    struct_hooks=[*DEFAULT_STRUCTURE_HOOKS, (UUID, structure_uuid)],
    unstruct_hooks=[*DEFAULT_UNSTRUCTURE_HOOKS_JSON, (UUID, str)],
)
```
