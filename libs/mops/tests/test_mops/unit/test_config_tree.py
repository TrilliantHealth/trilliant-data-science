import pytest

from thds.mops._utils.config_tree import ConfigTree, to_dotted_path


def test_basic_setv_getv():
    tree = ConfigTree("test")  # type: ignore
    tree.setv(42, "foo.bar")
    assert tree.getv("foo.bar") == 42
    assert tree.getv("foo.bar.baz") == 42  # should inherit from parent


def test_set_root_config_with_empty_string():
    tree = ConfigTree("emptystr")  # type: ignore
    tree.setv(42, "")
    assert tree.getv("") == 42
    assert tree.getv("foo.bar") == 42  # should inherit from parent


def test_masking():
    tree = ConfigTree("test")  # type: ignore
    tree.setv(1, "foo")
    tree.setv(2, "foo.bar")
    tree.setv(3, "foo", mask=True)
    assert tree.getv("foo") == 3
    assert tree.getv("foo.bar") == 3  # masked value should override even foo.bar=2
    assert tree.getv("foo.bar.baz") == 3


def test_mask_literally_everything_with_empty_string():
    tree = ConfigTree("test")  # type: ignore
    tree.setv(1, "foo")
    tree.setv(2, "foo.bar")
    tree.setv(9, "", mask=True)
    assert tree.getv("foo") == 9
    assert tree.getv("foo.bar") == 9
    assert tree.getv("foo.bar.baz") == 9


def test_parser():
    tree = ConfigTree("test", parse=int)  # type: ignore
    tree.setv("42", "foo")  # type: ignore
    assert tree.getv("foo") == 42


def test_setv_twice_is_fine():
    tree = ConfigTree("test")  # type: ignore
    tree.setv(1)
    tree.setv(2)
    assert tree.getv(to_dotted_path(test_setv_twice_is_fine)) == 2


def test_default_value():
    tree = ConfigTree("test")  # type: ignore
    assert tree.getv("nonexistent", default=99) == 99
    with pytest.raises(RuntimeError):
        tree.getv("nonexistent")


def test_dict_interface():
    tree = ConfigTree("test")  # type: ignore
    tree["foo.bar"] = 42
    assert tree.getv("foo.bar") == 42


def test_load_config():
    tree = ConfigTree("test")  # type: ignore
    config = {"foo.test": 1, "bar.test": 2, "__mask.baz.test": 3, "baz.not_our_config": 8}
    tree.load_config(config)
    assert tree.getv("foo") == 1
    assert tree.getv("bar") == 2
    assert tree.getv("baz.anything") == 3  # masked value


def test_config_using_module_itself():
    import sys

    our_module = sys.modules[__name__]
    tree = ConfigTree("test")  # type: ignore
    tree.setv(42, our_module)
    assert tree.getv(to_dotted_path(our_module)) == 42


def test_repr():
    tree = ConfigTree("test")  # type: ignore
    tree.setv(42, "foo.bar")
    assert repr(tree) == "ConfigTree('test', [('foo.bar', ConfigItem('foo.bar', 42))])"
