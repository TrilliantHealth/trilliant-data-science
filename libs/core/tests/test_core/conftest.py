import pytest


@pytest.fixture(autouse=True)
def clear_caches():
    from thds.core import meta

    meta.read_metadata.cache_clear()
    meta.get_version.cache_clear()
    meta.get_base_package.cache_clear()
