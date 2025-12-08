"""Utils for collecting structured documentation from class docstrings"""

import inspect
from collections import ChainMap
from itertools import chain
from typing import Callable, Dict, List, Literal, Optional, Set, Type

from docstring_parser import Docstring, DocstringMeta, DocstringParam, Style, parse

from .type_utils import bases, get_origin

DocCombineSpec = Literal["first", "join"]


def get_record_class_fields(cls: Type) -> Set[str]:
    """Get the names of the attributes of a class"""

    if not inspect.isclass(cls):
        raise ValueError("Input should be a class")

    return set(inspect.signature(cls).parameters.keys())


def record_class_docs(
    cls: Type,
    filter_bases: Optional[Callable[[Type], bool]] = None,
    style: Style = Style.AUTO,
    combine_docs: DocCombineSpec = "first",
    join_sep: str = "\n\n",
    require_complete: bool = False,
) -> Docstring:
    cls = get_origin(cls) or cls  # unwrap parameterized generics
    base_clss = [c for c in bases(cls, filter_bases) if c is not object]

    docs = [parse(c.__doc__, style=style) for c in base_clss if c.__doc__]
    params = dict(ChainMap(*({param.arg_name: param for param in doc.params} for doc in docs)))

    if require_complete:
        missing = get_record_class_fields(cls) - set(params.keys())
        if missing:
            raise ValueError(f"Missing docstring params for {cls.__name__}: {missing}")

    if combine_docs == "first":
        short_description = next((doc.short_description for doc in docs if doc.short_description), None)
        long_description = next((doc.long_description for doc in docs if doc.long_description), None)
    else:
        short_description = join_sep.join(
            doc.short_description for doc in reversed(docs) if doc.short_description
        )
        long_description = join_sep.join(
            doc.long_description for doc in reversed(docs) if doc.long_description
        )

    combined_meta: Dict[Type[DocstringMeta], List[DocstringMeta]] = {
        DocstringParam: list(params.values())
    }
    for doc in docs:
        metas: Dict[Type[DocstringMeta], List[DocstringMeta]] = {}
        for meta in doc.meta:
            meta_type = type(meta)
            if meta_type not in combined_meta:
                metas.setdefault(meta_type, []).append(meta)
        for meta_type, meta_list in metas.items():
            combined_meta[meta_type] = meta_list

    style_: Optional[Style] = next((doc.style for doc in docs if doc.style), None)
    combined_doc = Docstring(style_)
    combined_doc.short_description = short_description
    combined_doc.long_description = long_description
    combined_doc.meta = list(chain.from_iterable(combined_meta.values()))

    bad_params = {param.arg_name for param in combined_doc.params if not param.description}
    if bad_params:
        raise ValueError(f"Parameters {bad_params} have no description")

    return combined_doc
