import pytest

from .test_mops import config as internal_config


def pytest_collection_modifyitems(items):
    for item in items:
        item.add_marker(pytest.mark.integration)


def pytest_addoption(parser):
    group = parser.getgroup("mops")
    group.addoption(
        "--test-uri-root",
        action="store",
        type=str,
        help="Where you want to put mops control files during the tests. Should be an adls:// URI if you have adls access.",
        default=internal_config.TEST_TMP_URI,
    )


def pytest_configure(config):
    uri = config.getoption("--test-uri-root")
    if not uri.endswith("/"):
        uri += "/"
    internal_config.TEST_TMP_URI = uri
