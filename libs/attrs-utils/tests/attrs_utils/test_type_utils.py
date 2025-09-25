import typing as ty
from typing import List, Type, Union

import attrs
import pytest
from typing_extensions import Annotated

from thds.attrs_utils.type_utils import (
    bases,
    is_collection_type,
    is_mapping_type,
    is_namedtuple_type,
    is_set_type,
    is_variadic_tuple_type,
    literal_base,
    newtype_base,
    typename,
    unwrap_annotated,
    unwrap_optional,
)

from . import conftest as types


@pytest.mark.parametrize(
    "newtype, expected_base",
    [
        pytest.param(str, str, id="not a newtype"),
        pytest.param(types.SimpleNewType, int, id="basic newtype"),
        pytest.param(types.NestedNewType, int, id="singly nested newtype"),
        pytest.param(types.DoublyNestedNewType, int, id="doubly nested newtype"),
    ],
)
def test_newtype_base(newtype: Type, expected_base: Type):
    base = newtype_base(newtype)
    assert base is expected_base, (newtype, base, expected_base)


@pytest.mark.parametrize(
    "optional_type, expected_base",
    [
        pytest.param(int, int, id="not an optional type"),
        pytest.param(types.OptionalType, str, id="simple optional type"),
        pytest.param(types.DoublyOptionalType, str, id="twice-optional type"),
        pytest.param(types.OptionalUnion, Union[int, str], id="optional union type"),
    ],
)
def test_unwrap_optional(optional_type: Type, expected_base: Type):
    base = unwrap_optional(optional_type)
    assert base == expected_base, (optional_type, base, expected_base)


@pytest.mark.parametrize(
    "literal_type, expected_base",
    [
        pytest.param(types.LiteralInt, int, id="literal int type"),
        pytest.param(types.LiteralStr, str, id="literal str type"),
        pytest.param(types.LiteralMixed, Union[int, str], id="literal of mixed type"),
    ],
)
def test_literal_base(literal_type: Type, expected_base: Type):
    base = literal_base(literal_type)
    # we use == rather than `is` here because sometimes `Union[t1, t2] is Union[t1, t2]` fails
    assert base == expected_base, (literal_type, base, expected_base)


@pytest.mark.parametrize(
    "type, expected_name",
    [
        pytest.param(types.TV, "TV", id="type variable"),
        pytest.param(types.RecordType, "RecordType", id="attrs record type"),
        pytest.param(types.SimpleNewType, "SimpleNewType", id="simple newtype"),
        pytest.param(types.DoublyNestedNewType, "DoublyNestedNewType", id="doubly nested newtype"),
    ],
)
def test_typename(type: Type, expected_name: str):
    name = typename(type)
    assert name == expected_name, (type, name, expected_name)


@pytest.mark.parametrize(
    "type, is_collection_expected",
    [
        pytest.param(str, False, id="str is not collection"),
        pytest.param(bytes, False, id="bytes is not collection"),
        pytest.param(bytearray, False, id="bytearray is not collection"),
        pytest.param(ty.List, True, id="unparameterized List is collection"),
        pytest.param(ty.List[types.RecordType], True, id="parameterized List is collection"),
        pytest.param(ty.Tuple, True, id="unparameterized Tuple is collection"),
        pytest.param(ty.Tuple[int, str], True, id="parameterized Tuple is collection"),
        pytest.param(ty.Tuple[str, ...], True, id="parameterized variadic Tuple is collection"),
        pytest.param(ty.Sequence, True, id="unparameterized Sequence is collection"),
        pytest.param(ty.Sequence[types.TV], True, id="parameterized Sequence is collection"),  # type: ignore
        pytest.param(ty.Collection, True, id="unparameterized Collection is collection"),
        pytest.param(
            ty.Collection[types.SimpleNewType], True, id="parameterized Collection is collection"
        ),
    ],
)
def test_is_collection_type(type: Type, is_collection_expected: bool):
    is_collection = is_collection_type(type)
    assert is_collection == is_collection_expected, (type, is_collection, is_collection_expected)


@pytest.mark.parametrize(
    "type, is_set_expected",
    [
        pytest.param(str, False, id="str is not set"),
        pytest.param(bytes, False, id="bytes is not set"),
        pytest.param(bytearray, False, id="bytearray is not set"),
        pytest.param(ty.List, False, id="unparameterized List is not set"),
        pytest.param(ty.List[types.RecordType], False, id="parameterized List is not set"),
        pytest.param(ty.AbstractSet, True, id="unparameterized AbstractSet is set"),
        pytest.param(ty.Set, True, id="unparameterized Set is set"),
        pytest.param(ty.MutableSet[str], True, id="parameterized MutableSet is set"),
        pytest.param(ty.FrozenSet[str], True, id="parameterized Frozenset is set"),
        pytest.param(ty.Set[types.SimpleNewType], True, id="parameterized set is set"),
        pytest.param(ty.Sequence[ty.Set[str]], False, id="parameterized Sequence of set is not set"),
    ],
)
def test_is_set_type(type: Type, is_set_expected: bool):
    is_set = is_set_type(type)
    assert is_set == is_set_expected, (type, is_set, is_set_expected)


@pytest.mark.parametrize(
    "type, is_mapping_expected",
    [
        pytest.param(types.RecordType, False, id="record type is not mapping"),
        pytest.param(ty.Set, False, id="Set is not mapping"),
        pytest.param(ty.Collection, False, id="unparameterized Collection is not mapping"),
        pytest.param(
            ty.Collection[types.SimpleNewType], False, id="parameterized Collection is not mapping"
        ),
        pytest.param(ty.Dict, True, id="unparameterized Dict is mapping"),
        pytest.param(ty.Dict[str, types.RecordType], True, id="parameterized Dict is mapping"),
        pytest.param(ty.Mapping, True, id="unparameterized Mapping is mapping"),
        pytest.param(ty.Mapping[types.TV, ty.List], True, id="parameterized Mapping is mapping"),  # type: ignore
        pytest.param(ty.MutableMapping, True, id="unparameterized MutableMapping is mapping"),
        pytest.param(ty.MutableMapping[int, str], True, id="parameterized MutableMapping is mapping"),
        pytest.param(ty.OrderedDict, True, id="unparameterized OrderedDict is mapping"),
        pytest.param(
            ty.OrderedDict[types.NestedNewType, bool], True, id="parameterized OrderedDict is mapping"
        ),
        pytest.param(ty.DefaultDict, True, id="unparameterized DefaultDict is mapping"),
        pytest.param(
            ty.DefaultDict[types.LiteralStr, int], True, id="parameterized DefaultDict is mapping"
        ),
    ],
)
def test_is_mapping_type(type: Type, is_mapping_expected: bool):
    is_mapping = is_mapping_type(type)
    assert is_mapping == is_mapping_expected, (type, is_mapping, is_mapping_expected)


@pytest.mark.parametrize(
    "type, is_namedtuple_type_expected",
    [
        pytest.param(tuple, False, id="tuple is not a namedtuple"),
        pytest.param(ty.Tuple, False, id="Tuple is not a namedtuple"),
        pytest.param(ty.Tuple[types.NT, str], False, id="parameterized Tuple is not a namedtuple"),
        pytest.param(
            ty.Tuple[types.NT, ...], False, id="variadic parameterized Tuple is not a namedtuple"
        ),
        pytest.param(types.NT, True, id="NamedTuple subclass is a namedtuple"),
        pytest.param(types.NTDynamic, True, id="dynmamically constructed NamedTuple is a namedtuple"),
    ],
)
def test_is_namedtuple_type(type: Type, is_namedtuple_type_expected: bool):
    is_namedtuple = is_namedtuple_type(type)
    assert is_namedtuple == is_namedtuple_type_expected, (
        type,
        is_namedtuple,
        is_namedtuple_type_expected,
    )


@pytest.mark.parametrize(
    "type, is_variadic_tuple_expected",
    [
        pytest.param(ty.Tuple[int, str], False, id="2-length tuple"),
        pytest.param(ty.Tuple[int, str, bool], False, id="3-length tuple"),
        pytest.param(ty.Tuple[ty.Tuple[int, str], bool], False, id="2-length nested tuple"),
        pytest.param(ty.Tuple[str, ...], True, id="variadic tuple"),
        pytest.param(ty.Tuple[types.NT, ...], True, id="variadic nested tuple"),
        pytest.param(ty.Collection[str], False, id="collection but not a tuple"),
    ],
)
def test_is_variadic_tuple_type(type: Type, is_variadic_tuple_expected: bool):
    is_variadic_tuple = is_variadic_tuple_type(type)
    assert is_variadic_tuple == is_variadic_tuple_expected, (
        type,
        is_variadic_tuple,
        is_variadic_tuple_expected,
    )


@pytest.mark.parametrize(
    "type, expected_unwrapped",
    [
        pytest.param(int, int, id="unannotated builtin type"),
        pytest.param(
            ty.List[types.NTDynamic], ty.List[types.NTDynamic], id="unannotated parameterized type"
        ),
        pytest.param(ty.Union[int, str], ty.Union[int, str], id="unannotated union type"),
        pytest.param(Annotated[int, "annotated int"], int, id="annotated builtin type"),
        pytest.param(
            Annotated[int, "annotated", "int"], int, id="annotated builtin type with multiple metadata"
        ),
        pytest.param(Annotated[types.NT, "named", "tuple"], types.NT, id="annotated custom type"),
        pytest.param(
            Annotated[ty.List[int], "list", "of int"], ty.List[int], id="annotated parameterized type"
        ),
    ],
)
def test_unwrap_annotated(type: Type, expected_unwrapped: Type):
    unwrapped = unwrap_annotated(type)
    assert unwrapped is expected_unwrapped, (type, unwrapped, expected_unwrapped)


@pytest.mark.parametrize(
    "type, predicate, expected_bases",
    [
        (types.RecordType, None, [types.RecordType, object]),
        (types.AttrsInherited, None, [types.AttrsInherited, types.RecordType, object]),
        (
            types.AttrsInheritedAgain,
            None,
            [types.AttrsInheritedAgain, types.AttrsInherited, types.RecordType, object],
        ),
        (types.RecordType, attrs.has, [types.RecordType]),
        (types.AttrsInherited, attrs.has, [types.AttrsInherited, types.RecordType]),
        (
            types.AttrsInheritedAgain,
            attrs.has,
            [types.AttrsInheritedAgain, types.AttrsInherited, types.RecordType],
        ),
    ],
)
def test_bases(
    type: Type, predicate: ty.Optional[ty.Callable[[Type], bool]], expected_bases: List[Type]
):
    actual_bases = bases(type, predicate)
    assert actual_bases == expected_bases, (type, actual_bases, expected_bases)
