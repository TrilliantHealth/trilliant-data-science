from thds.adls.resource import AdlsHashedResource


def test_serde():
    serialized = '{"uri": "adls://foo/bar/baz", "md5b64": "WPMVPiXYwhMrMjF87w3GvA=="}'
    assert AdlsHashedResource.parse(serialized).serialized == serialized
