from collections.abc import Generator

from ..conftest import ReferenceDataTestCase


def test_attrs_sqlite_loader(test_case_with_attrs_sqlite_module: ReferenceDataTestCase):
    test_case = test_case_with_attrs_sqlite_module
    assert test_case.schema is not None
    assert test_case.attrs_sqlite_module is not None
    for table in test_case.schema.package_tables:
        if table.run_time_installed:
            continue
        unq_constraints = {frozenset(c.unique) for c in table.unique_constraints}
        loader = test_case_with_attrs_sqlite_module.attrs_sqlite_loader_for(table.name)
        attrs_class = test_case_with_attrs_sqlite_module.attrs_class_for(table.name)
        if table.has_indexes:
            # should be a loader if there are indexes
            assert loader is not None
            tuples = test_case.tuples[table.name]

            # check the round trip from ground truth tuple PK values through query and back to tuple
            if table.primary_key:
                # find indexes of columns corresponding to PK
                columns = [c.name for c in table.columns]
                pk_ixs = [columns.index(name) for name in table.primary_key]
                for tup in tuples:
                    # pull out PK fields
                    pk = tuple(tup[i] for i in pk_ixs)
                    # query by PK
                    actual = loader.pk(*pk)
                    # construct a record from the raw ground truth tuple
                    expected = attrs_class(*tup)
                    # field-for-field equality check
                    assert actual == expected

                    if len(pk) == 1:
                        # test bulk method for single-field PKs
                        actual_bulk = loader.pk_bulk([pk[0], pk[0]])
                    else:
                        actual_bulk = loader.pk_bulk([pk, pk])

                    actual_bulk_list = list(actual_bulk)
                    assert actual_bulk_list == [expected]

            # check the round trip from ground truth tuple index field values through query and back to
            # tuples - note, we check for containment because there may be multiple results
            for index in table.indexes:
                is_unique = frozenset(index) in unq_constraints
                # check for corresponding index method
                method_name = "_".join(["idx", *index])
                assert hasattr(loader, method_name)
                method = getattr(loader, method_name)
                assert callable(method)

                method_name_bulk = f"{method_name}_bulk"
                assert hasattr(loader, method_name_bulk)
                method_bulk = getattr(loader, method_name_bulk)
                assert callable(method_bulk)

                # find indices of the index columns in the tuples
                idx_ixs = [columns.index(name) for name in index]
                for tup in tuples:
                    # pull out index fields
                    idx = tuple(tup[i] for i in idx_ixs)
                    # query by index
                    result = method(*idx)
                    # construct a record from the ground truth tuple
                    expected = attrs_class(*tup)
                    if is_unique:
                        assert expected == result
                    else:
                        # containment check
                        assert expected in result

                    if len(index) == 1:
                        # test bulk method for single-field indexes
                        result_bulk = method_bulk([idx[0], idx[0]])
                    else:
                        result_bulk = method_bulk([idx, idx])

                    assert isinstance(result_bulk, Generator)
                    result_bulk_list = list(result_bulk)
                    assert expected in result_bulk_list
        else:
            # should be no loader if there are no indexes
            assert loader is None
