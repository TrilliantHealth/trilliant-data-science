import ast

import pytest

from thds.tabularasa.schema.compilation.io import ast_eq

mod = ast.parse(
    """import typing
class Foo(typing.NamedTuple):
    x: typing.Optional[int] = 1
    y: typing.Union[int, List[str], None] = ["asdf"]
"""
)

mod_diff_format = ast.parse(
    """import typing

class Foo(typing.NamedTuple):

    x: typing.Optional[
        int
    ] = 1
    y: typing.Union[
        int, List[str], None
    ] = [
        "asdf"
    ]
"""
)

mod_comment = ast.parse(
    """import typing

class Foo(typing.NamedTuple):
    # just a comment
    x: typing.Optional[int] = 1
    y: typing.Union[
        int, List[str], None
    ] = [
        "asdf"
    ]
"""
)

mod_docstring = ast.parse(
    """import typing
class Foo(typing.NamedTuple):
    '''docstring'''
    x: typing.Optional[int] = 1
    y: typing.Union[int, List[str], None] = ["asdf"]
"""
)

mod_diff_type = ast.parse(
    """import typing
class Foo(typing.NamedTuple):
    x: typing.Optional[int] = 1.0
    y: typing.Union[int, List[str], None] = ["asdf"]
"""
)

mod_diff_value = ast.parse(
    """import typing
class Foo(typing.NamedTuple):
    x: typing.Optional[int] = 1
    y: typing.Union[int, List[str], None] = ["asdfg"]
"""
)


@pytest.mark.parametrize("mod1,mod2", [(mod, mod), (mod, mod_diff_format), (mod, mod_comment)])
def test_ast_eq(mod1, mod2):
    assert ast_eq(mod1, mod2)


@pytest.mark.parametrize("mod1,mod2", [(mod, mod_docstring), (mod, mod_diff_type), (mod, mod_diff_type)])
def test_ast_not_eq(mod1, mod2):
    assert not ast_eq(mod1, mod2)
