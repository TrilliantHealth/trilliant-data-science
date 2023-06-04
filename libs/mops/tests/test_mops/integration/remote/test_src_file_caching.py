from thds.adls import AdlsFqn
from thds.mops.remote.adls_remote_files import adls_remote_src

from ._util import arf


def test_remote_src_file_still_caches():
    fqn = AdlsFqn.of(
        "thdsdatasets", "prod-datasets", "test/read-only/only-for-use-by-mops-srcfile-caching-test.txt"
    )
    rs = adls_remote_src(*fqn, md5b64="U3vtigRGuroWtJFEQ5dKoQ==")

    with rs as p:
        # download has happened
        cp = arf._srcfile_cache.path(fqn)
        # on CI, if this is resolve instead of absolute, it will resolve the symlink and we'll lose the 'true' path
        assert cp.absolute() != p.absolute()
        assert cp.exists()
        assert p.exists()
        assert "test/read-only" in str(cp)
