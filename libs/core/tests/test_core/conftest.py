import pytest


@pytest.fixture(autouse=True)
def clear_caches():
    from thds.core.meta import read_metadata

    read_metadata.cache_clear()
