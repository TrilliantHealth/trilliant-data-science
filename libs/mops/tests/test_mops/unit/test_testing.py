import contextlib
import sys
import typing as ty

import pytest

from thds.mops.testing.deferred_imports import (
    assert_dev_deps_not_imported,
    module_name_re,
    module_names_from_import_statement,
)


@pytest.mark.parametrize(
    "names, matches, nonmatches",
    [
        pytest.param(
            ["foo"], ["foo.bar", "foo.bar.baz", "foo.bar.baz.quux"], [], id="submodules are matched"
        ),
        pytest.param(
            ["foo"], [], ["fool", "fool.bard"], id="modules with a common prefix are not matched"
        ),
        pytest.param(
            ["foo.bar"],
            ["foo.bar.baz", "foo.bar.baz.quux"],
            [],
            id="submodules are matched on nested names",
        ),
        pytest.param(
            ["foo.bar"], [], ["foo", "foo.baz"], id="parent and sibling modules are not matched"
        ),
        pytest.param(
            ["foo.bar.baz"],
            [],
            ["foo.bar", "foo.bar.quux"],
            id="parent, uncle, and sibling modules are not matched on nested names",
        ),
        pytest.param(
            ["foo.bar"],
            [],
            ["foo.bard", "fool.bar"],
            id="modules with a common prefix are not matched on nested names",
        ),
        pytest.param(
            ["foo.bar.baz"],
            [],
            ["foo.bar.bazz", "foo.bart.baz", "fool.bar.baz"],
            id="modules with a common prefix are not matched on nested names",
        ),
    ],
)
def test_module_name_re(
    names: ty.Collection[str], matches: ty.Collection[str], nonmatches: ty.Collection[str]
):
    re = module_name_re(names)
    for name in matches:
        assert re.match(name), name
    for name in nonmatches:
        assert not re.match(name), name


@pytest.mark.parametrize(
    "import_statement, module_names",
    [
        pytest.param("import foo", {"foo"}, id="simple base module import"),
        pytest.param("import foo.bar", {"foo", "foo.bar"}, id="simple nested module import"),
        pytest.param("import foo.bar as baz", {"foo", "foo.bar"}, id="nested module import with alias"),
        pytest.param(
            "import foo.bar.baz as quux",
            {"foo", "foo.bar", "foo.bar.baz"},
            id="doubly nested module import with alias",
        ),
        pytest.param("from foo import bar, baz", {"foo"}, id="base module 'from' imports"),
        pytest.param(
            "from foo.bar import baz, quux", {"foo", "foo.bar"}, id="nested module 'from' imports"
        ),
        pytest.param(
            "from foo.bar.baz import quux",
            {"foo", "foo.bar", "foo.bar.baz"},
            id="doubly nested module 'from' imports",
        ),
    ],
)
def test_module_names_from_import_statement(import_statement: str, module_names: ty.Set[str]):
    extracted = module_names_from_import_statement(import_statement)
    assert extracted == module_names, (import_statement, extracted, module_names)


def assert_is_module(name: str):
    try:
        exec(f"import {name}", {}, {})
    except ImportError as e:
        raise AssertionError(
            f"{name} is not a known module in this env; testing for its presence or absence in"
            f"sys.modules is meaningless"
        ) from e


@pytest.mark.parametrize(
    "import_statement, forbidden_modules, passing",
    [
        pytest.param("import os", ["os"], False, id="'import os' imports os"),
        pytest.param(
            "import os",
            ["os"],
            False,
            id="'import os' imports os even when os has already been imported",
        ),
        pytest.param(
            "from thds.core.log import getLogger",
            ["logging"],
            False,
            id="importing getLogger from thds.core.log imports logging",
        ),
        pytest.param("import typing", ["os"], True, id="'import typing' does not import os"),
        pytest.param(
            "from pathlib import Path",
            ["site"],
            True,
            id="'from pathlib import Path' does not import site",
        ),
        pytest.param(
            "import collections",
            ["importlib"],
            True,
            id="'import collections' does not import importlib",
        ),
        pytest.param("import thds", ["thds.mops"], True, id="'import thds' does not import thds.mops"),
        pytest.param(
            "import thds.mops",
            ["thds.mops.config", "thds.mops.pure"],
            True,
            id="'import thds.mops' does not import submodules",
        ),
    ],
)
def test_assert_dev_deps_not_imported(
    import_statement: str, forbidden_modules: ty.Collection[str], passing: bool
):
    imported_now = set(sys.modules)
    with contextlib.nullcontext() if passing else pytest.raises(AssertionError):
        assert_dev_deps_not_imported(import_statement, forbidden_modules)

    imported_then = set(sys.modules)
    assert imported_then.issuperset(imported_now), "some modules were deleted from the cache in testing"

    for name in forbidden_modules:
        assert_is_module(name)
