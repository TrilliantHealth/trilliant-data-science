from pandas.testing import assert_frame_equal

from thds.tabularasa.loaders.util import PandasParquetLoader

from ..conftest import ReferenceDataTestCase


def test_loader_dataframe_equals(test_case_with_pandas_module: ReferenceDataTestCase):
    test_case = test_case_with_pandas_module
    assert test_case.schema is not None
    assert test_case.pandas_module is not None
    for table in test_case.schema.package_tables:
        df_expected = test_case.dataframes[table.name]
        static_loader: PandasParquetLoader = test_case.pandas_parquet_loader_for(table.name)
        dynamic_loader: PandasParquetLoader = test_case.dynamic_pandas_parquet_loader_for(table.name)
        for loader in static_loader, dynamic_loader:
            df = loader(validate=True, postprocess=True)
            assert_frame_equal(
                df,
                df_expected,
                check_dtype=True,
                check_index_type=True,  # type: ignore
                check_exact=True,
                check_categorical=True,
            )
