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


def _set_env_for(monkeypatch, config_name: str, raw: str) -> None:
    """Mirrors `_sanitize_env`: dots become underscores, and the shell-friendly spelling
    is the uppercase one."""
    monkeypatch.setenv(config_name.replace(".", "_").upper(), raw)


@pytest.mark.parametrize(
    "raw,expected",
    [
        pytest.param("false", False, id="false"),
        pytest.param("False", False, id="mixed-case-false"),
        pytest.param("0", False, id="zero"),
        pytest.param("no", False, id="no"),
        pytest.param("off", False, id="off"),
        pytest.param("true", True, id="true"),
        pytest.param("1", True, id="one"),
        pytest.param("yes", True, id="yes"),
    ],
)
def test_a_bool_default_reads_the_word_in_its_environment_variable(monkeypatch, raw, expected):
    """`bool` is not self-parsing: every non-empty string is truthy, so inferring it from a
    bool default left these items impossible to turn off from a shell."""
    name = f"test.bool.inferred.{raw}"  # unique per case: the registry rejects duplicates
    _set_env_for(monkeypatch, name, raw)
    assert config.item(name, default=True)() is expected


@pytest.mark.parametrize(
    "spelling,parse_kwargs",
    [
        pytest.param("inferred", {}, id="inferred-from-the-default"),
        pytest.param("bool", {"parse": bool}, id="parse-bool"),
        pytest.param("tobool", {"parse": config.tobool}, id="parse-tobool"),
    ],
)
def test_every_way_of_declaring_a_boolean_reads_the_word(monkeypatch, spelling, parse_kwargs):
    """Three spellings reach the same item: inferring from a bool default, passing `bool`,
    and passing `tobool` explicitly - which most call sites do. They must agree.

    `parse=bool` taken literally is a truthiness test over the raw string, which no caller
    wants; it can only have meant "this is a boolean".
    """
    name = f"test.bool.spelling.{spelling}"
    _set_env_for(monkeypatch, name, "off")
    item = config.item(name, default=True, **parse_kwargs)

    assert item.parse is config.tobool, "every boolean item should share one parser"
    assert item() is False


def test_an_unset_bool_keeps_its_default(monkeypatch):
    """An empty environment variable is treated as unset, so it cannot mean False."""
    monkeypatch.delenv("TEST_BOOL_UNSET", raising=False)
    assert config.item("test.bool.unset", default=True)() is True


def test_non_bool_types_still_parse_themselves(monkeypatch):
    """The bool special case must not disturb the self-parsing types around it."""
    _set_env_for(monkeypatch, "test.int.self", "7")
    _set_env_for(monkeypatch, "test.str.self", "seven")
    assert config.item("test.int.self", default=1)() == 7
    assert config.item("test.str.self", default="one")() == "seven"
