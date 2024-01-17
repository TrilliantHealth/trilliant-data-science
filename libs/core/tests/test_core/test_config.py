import pytest

from thds.core import config

A = config.item("A", 1)
B = config.item("B", 2)
C = config.item("C", 3)
D = config.item("D", 4)


def test_recursive_config_load():
    config.set_global_defaults(
        {
            "tests.test_core.test_config.A": 10,
            "tests": {"test_core": {"test_config": {"C": 20}}},
        }
    )
    assert A() == 10
    assert C() == 20
    assert B() == 2
    assert D() == 4


def test_set_global_defaults_error_when_module_exists_but_config_doesnt():
    with pytest.raises(KeyError, match="Config item tests.test_core.test_config.E is not registered"):
        config.set_global_defaults({"tests.test_core.test_config.E": 10})


def test_set_global_defaults_works_when_module_doesnt_exist():
    # we support fully-dynamic config values.
    config.set_global_defaults({"foo.bar.baz": 10})
    assert config.config_by_name("foo.bar.baz")() == 10
