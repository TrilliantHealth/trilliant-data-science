import pytest

from thds.core import config

_cfg = config.in_module(__name__)

A = _cfg("A", 1)
B = _cfg("B", 2)
C = _cfg("C", 3)
D = _cfg("D", 4)


def test_recursive_config_load():
    config.set_global_defaults(
        {
            "tests.test_core.test_config.A": 10,
            "tests": {"test_core": {"test_config": {"C": 20}}},
        }
    )


def test_set_global_defaults_error():
    with pytest.raises(KeyError, match="Config item tests.test_core.test_config.E is not registered."):
        config.set_global_defaults({"tests.test_core.test_config.E": 10})
