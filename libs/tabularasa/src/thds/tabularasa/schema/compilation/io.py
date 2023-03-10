import ast
from functools import singledispatch
from itertools import starmap, zip_longest
from logging import getLogger
from operator import itemgetter
from pathlib import Path
from typing import Any, Iterator, List, Tuple, Union

_LOGGER = getLogger(__name__)

AST_CODE_CONTEXT_VARS = {"lineno", "col_offset", "ctx", "end_lineno", "end_col_offset"}


def ast_eq(ast1: ast.AST, ast2: ast.AST) -> bool:
    """Return True if two python source strings are AST-equivalent, else False"""
    return _ast_eq(ast1, ast2)


def ast_vars(node: ast.AST) -> Iterator[Tuple[str, Any]]:
    """Iterator of (name, value) tuples for all attributes of an AST node *except* for those that are
    not abstract (e.g. line numbers and column offsets)"""
    return (
        (name, value)
        for name, value in sorted(vars(node).items(), key=itemgetter(0))
        if name not in AST_CODE_CONTEXT_VARS
    )


@singledispatch
def _ast_eq(ast1: Any, ast2: Any) -> bool:
    # base case, literal values (non-AST nodes)
    return (type(ast1) is type(ast2)) and (ast1 == ast2)


@_ast_eq.register(ast.AST)
def _ast_eq_ast(ast1: ast.AST, ast2: ast.AST) -> bool:
    if type(ast1) is not type(ast2):
        return False
    attrs1 = ast_vars(ast1)
    attrs2 = ast_vars(ast2)
    return all(
        (name1 == name2) and _ast_eq(attr1, attr2)
        for (name1, attr1), (name2, attr2) in zip(
            attrs1,
            attrs2,
        )
    )


@_ast_eq.register(list)
@_ast_eq.register(tuple)
def _ast_eq_list(ast1: List[Any], ast2: List[Any]):
    missing = object()
    return all(starmap(_ast_eq, zip_longest(ast1, ast2, fillvalue=missing)))


def write_if_ast_changed(source: str, path: Union[str, Path]):  # pragma: no cover
    """Write the source code `source` to the file at `path`, but only if the file doesn't exist, or the
    AST of the code therein differs from that of `source`"""
    path = Path(path)
    this_ast = ast.parse(source)

    if path.exists():
        with open(path, "r+") as f:
            that_source = f.read()
            try:
                that_ast = ast.parse(that_source)
            except SyntaxError:
                _LOGGER.warning(
                    f"syntax error in code at {path}; merge conflicts? Code will be overwritten"
                )
                rewrite = True
                reason = "Invalid AST"
            else:
                rewrite = not ast_eq(this_ast, that_ast)
                reason = "AST changed"

            if rewrite:
                _LOGGER.info(f"writing new generated code to {path}; {reason}")
                f.seek(0)
                f.truncate()
                f.write(source)
            else:
                _LOGGER.info(f"leaving generated code at {path}; AST unchanged")
    else:
        _LOGGER.info(f"writing new generated code to {path}; no prior file")
        with open(path, "w") as f:
            f.write(source)


def write_sql(source: str, path: Union[str, Path]):  # pragma: no cover
    """Write the SQL source code `source` to the file at `path`"""
    path = Path(path)
    _LOGGER.info(f"writing new generated code to {path}; no prior file")
    with open(path, "w") as f:
        f.write(source)
