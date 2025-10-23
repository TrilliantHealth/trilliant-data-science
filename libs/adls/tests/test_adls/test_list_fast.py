import typing as ty

import pytest

from thds.adls.blob_meta import BlobMeta
from thds.adls.list_fast import multilayer_yield_sources


class Test_yield_all_blobs:
    """
    this test has special knowledge of some data that lives in
    `adls://thdsdatasets/prod-datasets/test/read-only`.  there's a cautionary
    note there telling people to never delete any of the files contained
    therein.  but if this test fails, the first thing you should do is look in
    ADLS for a folder that looks like this:

    a-bunch-of-parquet-files
    ├── 1.parquet
    ├── 2.parquet
    ├── a
    │   ├── 1.parquet
    │   └── not-a-parquet-file.txt
    ├── b
    │   └── 1.parquet
    └── c
        ├── x
        │   ├── 1.parquet
        │   ├── 2.parquet
        │   └── not-a-parquet-file.txt
        └── y
            ├── 1.parquet
            └── p
                └── 1.parquet

    7 directories, 10 files
    """

    @pytest.mark.parametrize(
        "filter_, expected",
        [
            (
                lambda blob: blob.path.endswith(".parquet"),
                {
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/1.parquet",
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/2.parquet",
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/a/1.parquet",
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/b/1.parquet",
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/c/x/1.parquet",
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/c/x/2.parquet",
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/c/y/1.parquet",
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/c/y/p/1.parquet",
                },
            ),
            (
                lambda blob: blob.path.endswith(".txt"),
                {
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/a/not-a-parquet-file.txt",
                    "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files/c/x/not-a-parquet-file.txt",
                },
            ),
        ],
    )
    def test_it_filters_on_match_suffix(
        self, filter_: ty.Callable[[BlobMeta], bool], expected: set[str]
    ):
        adls_dir = "adls://thdsdatasets/prod-datasets/test/read-only/a-bunch-of-parquet-files"

        uris = {src.uri for src in multilayer_yield_sources(adls_dir, filter_=filter_)}

        assert uris == expected
