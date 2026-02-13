from typing import Any, Dict, Iterable, List, Type

from thds.tabularasa.loaders.sqlite_util import (
    get_args,
    get_origin,
    is_literal_type,
    is_union_type,
    resolve_newtypes,
)
from thds.tabularasa.loaders.util import AttrsParquetLoader

from ..conftest import ReferenceDataTestCase

NUM_ATTRS_TEST_ROWS = 100


def instancecheck(t):
    t = resolve_newtypes(t)
    if is_union_type(t):
        args = get_args(t)
        checks = tuple(instancecheck(a) for a in args)

        def check(value):
            return any(c(value) for c in checks)

    elif is_literal_type(t):
        possible_values = get_args(t)

        def check(value):
            return value in possible_values

    else:
        args = get_args(t)
        origin = get_origin(t)
        if args:
            if origin in (dict, Dict):
                keycheck, valcheck = instancecheck(args[0]), instancecheck(args[1])

                def check(value):
                    return isinstance(value, dict) and all(
                        keycheck(k) and valcheck(v) for k, v in value.items()
                    )

            elif origin in (list, List):
                valcheck = instancecheck(args[0])

                def check(value):
                    return isinstance(value, list) and all(valcheck(v) for v in value)

            else:
                raise TypeError(f"Can't derive type checker for generic type {t}")
        else:
            # all other types
            def check(value):
                return isinstance(value, t)

    return check


def test_attrs_loader(test_case_with_attrs_module: ReferenceDataTestCase):
    assert test_case_with_attrs_module.schema is not None
    assert test_case_with_attrs_module.schema.package_tables is not None
    for table in test_case_with_attrs_module.schema.package_tables:
        if table.run_time_installed:
            continue
        loader = test_case_with_attrs_module.attrs_loader_for(table.name)
        rows: Iterable[Any] = loader()
        schema = list(loader.type_.__annotations__.items())
        typecheckers = [(name, type_, instancecheck(type_)) for name, type_ in schema]

        def typecheck_error_message(table: str, row: int, column: str, type_: Type, value: Any) -> str:
            return (
                f"table '{table}', row {row}, column '{column}': {value!r} not an instance of {type_!r}"
            )

        for ix, row in zip(range(100), rows):
            assert isinstance(row, loader.type_)
            for name, type_, check in typecheckers:
                value = getattr(row, name)
                assert check(value), typecheck_error_message(loader.table_name, ix, name, type_, value)


def test_loader_rows_equal(test_case_with_attrs_module: ReferenceDataTestCase):
    test_case = test_case_with_attrs_module
    assert test_case.schema is not None
    assert test_case.attrs_module is not None
    for table in test_case.schema.package_tables:
        if table.name not in test_case.tuples:
            continue
        tuples = test_case.tuples[table.name]
        if table.primary_key:
            columns = [c.name for c in table.columns]
            pk_ixs = [columns.index(name) for name in table.primary_key]
            tuples = sorted(tuples, key=lambda tup: tuple(tup[ix] for ix in pk_ixs))  # noqa: B023

        loader: AttrsParquetLoader = test_case.attrs_loader_for(table.name)
        cls: Type = test_case.attrs_class_for(table.name)
        rows_expected = [cls(*tup) for tup in tuples]
        rows: List[Any] = list(loader())  # type: ignore
        assert rows == rows_expected
