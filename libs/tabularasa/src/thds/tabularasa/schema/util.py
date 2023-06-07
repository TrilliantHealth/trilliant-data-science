import functools
import importlib
import itertools
import typing
from typing import Callable, Iterable, List, Optional, Set, Union

import networkx as nx
import numpy as np
import pandas as pd
from pandas.core.dtypes import base as pd_dtypes
from pydantic import BaseModel, Extra, StrictFloat, StrictInt, StrictStr, constr

EnumList = Union[List[StrictInt], List[StrictFloat], List[StrictStr]]

_identifier_pattern = r"[a-zA-Z]\w*"
_dunder_identifier_pattern = r"[a-zA-Z_]\w*"
_dashed_identifier_pattern = rf"{_identifier_pattern}(-{_identifier_pattern})*"
_dotted_identifier_pattern = rf"{_identifier_pattern}(\.{_dunder_identifier_pattern})*"
_rel_path_pattern = r"[^/].*"  # paths are validated by the filesystem, not us.
_md5_hex_pattern = r"[0-9a-f]{32}"

if not typing.TYPE_CHECKING:
    # pydantic (hilariously) uses match instead of fullmatch, so we
    # have to anchor the regexes, but only at the end, since re.match
    # requires the match to be found at the beginning of the string.
    Identifier = constr(regex=_identifier_pattern + "$")
    DottedIdentifier = constr(regex=_dotted_identifier_pattern + "$")
    DashedIdentifier = constr(regex=_dashed_identifier_pattern + "$")
    PathStr = constr(regex=_rel_path_pattern + "$")
    HexStr = constr(regex=_md5_hex_pattern + "$")
    NonEmptyStr = constr(min_length=1)
else:
    Identifier = str
    DottedIdentifier = str
    DashedIdentifier = str
    PathStr = str
    HexStr = str
    NonEmptyStr = str


def snake_to_title(schema_name: str, separator: str = ""):
    """Turn a snake-case name from the schema into a title-case name with separated by `separator`."""
    parts = str(schema_name).split("_")
    return separator.join(part if part.isupper() else part.title() for part in parts)


def snake_case(schema_name: str) -> str:
    """Alias for `str.lower` but defined here as a single source of truth in case we change that.
    Names for tables, columns, and types in the schema should be underscore-separated, but tokens may be
    capitalized to indicated that in class names they should remain as such (e.g. acronyms - see
    `snake_to_title`)"""
    return schema_name.lower()


@functools.singledispatch
def render_dtype(dt: Union[np.dtype, pd_dtypes.ExtensionDtype]) -> str:
    raise NotImplementedError(f"Can't interpret {dt} as a pandas dtype")


@render_dtype.register(pd_dtypes.ExtensionDtype)
def render_pandas_dtype(dt: pd_dtypes.ExtensionDtype) -> str:
    return f"pd.{type(dt).__name__}()"


@render_dtype.register(pd.CategoricalDtype)
def render_pandas_categorical_dtype(dt: pd.CategoricalDtype) -> str:
    return f"pd.{pd.CategoricalDtype.__name__}({list(dt.categories)!r}, ordered={dt.ordered})"


@render_dtype.register(np.dtype)
def render_numpy_dtype(dt: np.dtype) -> str:
    return f'np.dtype("{dt.name}")'


def all_predecessors(g: nx.DiGraph, nodes: Iterable) -> Set:
    frontier = set(nodes)
    predecessors = set()
    while frontier:
        predecessors.update(frontier)
        frontier = set(itertools.chain.from_iterable(map(g.predecessors, frontier)))
    return predecessors


def all_successors(g: nx.DiGraph, nodes: Iterable) -> Set:
    return all_predecessors(nx.reverse(g), nodes)


def predecessor_graph(g: nx.DiGraph, nodes: Iterable) -> nx.DiGraph:
    predecessors = all_predecessors(g, nodes)
    return nx.induced_subgraph(g, predecessors)


def successor_graph(g: nx.DiGraph, nodes: Iterable) -> nx.DiGraph:
    successors = all_successors(g, nodes)
    return nx.induced_subgraph(g, successors)


def import_func(path: str) -> Callable:
    parts = path.split(".")
    module = ".".join(parts[:-1])
    name = parts[-1]
    mod = importlib.import_module(module)
    func = getattr(mod, name)
    if not callable(func):
        raise TypeError(f"value {func} of type {type(func)} is not callable")

    return func


class DocumentedMixin(BaseModel, extra=Extra.forbid):
    doc: Optional[str] = None
    markup: Optional[PathStr] = None

    @property
    def docstring(self) -> Optional[str]:
        if self.doc is None:
            if self.markup is None:
                return None
            else:
                with open(self.markup, "r") as f:
                    return f.read()
        else:
            return self.doc
