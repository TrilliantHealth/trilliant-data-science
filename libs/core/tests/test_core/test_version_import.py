from thds.core import __version__


def test_version_at_import() -> None:
    assert __version__
