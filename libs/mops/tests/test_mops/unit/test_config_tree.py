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


def test_load_from_config_file():
    tree = ConfigTree("foo.bar")  # type: ignore
    tree.setv(17, "a.c")
    tree.load_config(
        {
            "a": {
                "b": {"foo": {"bar": 42}},
                "c": {"__mask": {"foo": {"bar": 100}}},
            }
        }
    )

    assert tree.getv("a.c") == 100  # from the mask
    assert tree.getv("a.b") == 42  # from the non-masked config.


def test_load_config_does_not_override_more_specific_code_entry():
    """A config-file entry at package level does NOT beat a code-level entry at a
    more specific (e.g. function) path. This is by design — the hierarchical lookup
    prefers the most specific match, regardless of source. Use __mask to force an
    override of an entire subtree."""
    tree = ConfigTree("test")  # type: ignore
    tree.setv("from-decorator", "foo.bar.baz.my_func")
    tree.load_config({"foo.bar.test": "from-toml"})

    assert tree.getv("foo.bar.baz.my_func") == "from-decorator"
    # but less-specific paths DO pick up the config-file entry
    assert tree.getv("foo.bar.baz.other_func") == "from-toml"


def test_load_config_overrides_same_path_code_entry():
    """A config-file entry at the same path as a code-level entry overwrites it,
    because load_config calls setv which updates the existing ConfigItem."""
    tree = ConfigTree("test")  # type: ignore
    tree.setv("from-decorator", "foo.bar.baz.my_func")
    tree.load_config({"foo.bar.baz.my_func.test": "from-toml"})

    assert tree.getv("foo.bar.baz.my_func") == "from-toml"


def test_load_config_mask_overrides_more_specific_code_entry():
    """A __mask config-file entry at package level DOES beat a code-level entry at a
    more specific path. This is how users override an entire subtree from a TOML file."""
    tree = ConfigTree("test")  # type: ignore
    tree.setv("from-decorator", "foo.bar.baz.my_func")
    tree.load_config({"__mask.foo.bar.test": "from-toml-mask"})

    assert tree.getv("foo.bar.baz.my_func") == "from-toml-mask"
