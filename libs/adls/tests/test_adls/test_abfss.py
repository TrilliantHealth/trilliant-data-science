from thds.adls import AdlsFqn, abfss


def test_basic_abfss_formatting():
    assert (
        abfss.from_adls_fqn(AdlsFqn.parse("adls://foo/bar/baz"))
        == "abfss://bar@foo.dfs.core.windows.net/baz"
    )


def test_uses_root():
    assert (
        abfss.from_adls_fqn(AdlsFqn.parse("adls://foo/bar/baz").root().join("/ban/", "/zoo"))
        == "abfss://bar@foo.dfs.core.windows.net/ban/zoo"
    )


def test_to_adls_fqn():
    assert abfss.to_adls_fqn("abfss://bar@foo.dfs.core.windows.net/baz") == AdlsFqn.of(
        "foo", "bar", "baz"
    )


def test_from_adls_uri():
    assert abfss.from_adls_uri("adls://foo/bar/baz") == "abfss://bar@foo.dfs.core.windows.net/baz"
