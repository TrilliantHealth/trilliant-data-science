from thds.adls import AdlsRoot, defaults


def test_defaults():
    assert defaults.env_root() == AdlsRoot("thdsscratch", "tmp")
    assert defaults.env_root_uri() == "adls://thdsscratch/tmp/"

    assert defaults.env_root("prod") == AdlsRoot("thdsdatasets", "prod-datasets")
    assert defaults.env_root_uri("prod") == "adls://thdsdatasets/prod-datasets/"
