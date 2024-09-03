from thds.adls.fqn import AdlsFqn, AdlsRoot, format_fqn, parent, parse_fqn


def test_adls_fqn_basics():
    old_name = "thdsdatasets prod-datasets /lib-datamodel/v22/backerd compat.jsonl"
    name = "adls://thdsdatasets/prod-datasets/lib-datamodel/v22/backerd compat.jsonl"
    fqn = parse_fqn(old_name)

    assert fqn.sa == "thdsdatasets"
    assert fqn.container == "prod-datasets"
    assert fqn.path == "lib-datamodel/v22/backerd compat.jsonl"

    assert format_fqn(*fqn) == name

    assert format_fqn(fqn.sa, fqn.container, fqn.path) == name
    # we add the forward slash in front of the path.

    assert str(fqn) == name

    assert AdlsFqn.parse(name) == parse_fqn(name)
    assert AdlsFqn.parse(old_name) == parse_fqn(name)

    joined = parse_fqn("sa1 cont /path/to/dir/").join("/somedir").path
    assert "path/to/dir/somedir" in joined, joined  # no double slash
    assert "path/foo/bar/baz" in parse_fqn("sa2 cont2 path/foo/bar").join("baz").path


def test_adls_fqn_parent():
    base = AdlsFqn.of("thdsdatasets", "tmp", "foo/bar/baz")
    assert base.parent.path == "foo/bar"
    assert base.parent.parent.path == "foo"
    assert base.parent.parent.parent.path == ""
    assert base.parent.parent.parent.parent.path == ""

    base = AdlsFqn.of("thdsdatasets", "tmp", "foo-bar-baz")
    assert base.parent.path == ""
    assert base.parent.sa == "thdsdatasets"
    assert base.parent.container == "tmp"


def test_adls_root_parent():
    root = AdlsRoot("foo", "bar")
    assert root.parent is root
    assert parent(root) is root


def test_adls_root_parse():
    uri = "adls://foo/bar"
    assert AdlsRoot.parse(uri) == AdlsRoot("foo", "bar")
    assert str(AdlsRoot.parse(uri)) == uri + "/"
    assert AdlsRoot.parse(uri + "/") == AdlsRoot.parse(uri)
