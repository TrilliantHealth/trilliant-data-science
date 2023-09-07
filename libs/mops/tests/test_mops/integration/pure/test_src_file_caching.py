from thds.adls import AdlsFqn
from thds.mops.pure.adls import src

from ._util import dl


def test_remote_src_file_still_caches():
    fqn = AdlsFqn.of(
        "thdsdatasets", "prod-datasets", "test/read-only/only-for-use-by-mops-srcfile-caching-test.txt"
    )
    rs = src(fqn, md5b64="U3vtigRGuroWtJFEQ5dKoQ==")

    with rs as p:
        # download has happened
        cp = dl.srcfile_cache.path(fqn)
        # on CI, if this is resolve instead of absolute, it will resolve the symlink and we'll lose the 'true' path
        assert cp.absolute() != p.absolute()
        assert cp.exists()
        assert p.exists()
        assert "test/read-only" in str(cp)
