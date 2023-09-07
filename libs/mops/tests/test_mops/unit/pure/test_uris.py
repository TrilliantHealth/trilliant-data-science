from thds.adls.fqn import AdlsFqn, AdlsRoot
from thds.mops.pure.core.uris import to_lazy_uri


def test_to_lazy_uri():
    assert to_lazy_uri(AdlsRoot("thds", "tmp"))() == "adls://thds/tmp/"
    assert to_lazy_uri(AdlsFqn("thds", "tmp", "foobar/baz"))() == "adls://thds/tmp/foobar/baz"
    assert to_lazy_uri("adls://thds/tmp/foobar/baz")() == "adls://thds/tmp/foobar/baz"
    assert to_lazy_uri(lambda: "adls://thds/tmp/foobar/baz")() == "adls://thds/tmp/foobar/baz"
