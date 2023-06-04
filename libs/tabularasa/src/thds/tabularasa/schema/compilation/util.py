import itertools
import re
import textwrap
from inspect import Signature, signature
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, Union

import thds.tabularasa.schema.metaschema as metaschema

AUTOGEN_DISCLAIMER = "This code is auto-generated; do not edit!"


def sorted_class_names_for_import(names: Iterable[str]) -> List[str]:
    all_names = set(names)
    names_upper = [name for name in all_names if name.isupper()]
    class_names = all_names.difference(names_upper)
    return sorted(names_upper) + sorted(class_names, key=str.lower)


def _list_literal(exprs: Iterable[str], linebreak: bool = True) -> str:
    sep = ",\n    " if linebreak else ", "
    start = "\n    " if linebreak else ""
    end = ",\n" if linebreak else ""

    exprs = iter(exprs)
    try:
        peek = next(exprs)
    except StopIteration:
        return "[]"
    else:
        return f"[{start}{sep.join(itertools.chain((peek,), exprs))}{end}]"


def _dict_literal(named_exprs: Iterable[Tuple[str, str]], linebreak: bool = True):
    sep = ",\n    " if linebreak else ", "
    start = "\n    " if linebreak else ""
    end = ",\n" if linebreak else ""

    keyval = "%s=%s".__mod__
    named_exprs = iter(named_exprs)
    try:
        peek = next(named_exprs)
    except StopIteration:
        return "{}"
    else:
        return f"dict({start}{sep.join(map(keyval, itertools.chain((peek,), named_exprs)))}{end})" ""


def _indent(expr: str, level: int = 1, first_line: bool = False) -> str:
    ws = "    " * level
    indented = textwrap.indent(expr, ws)
    return indented if first_line else indented.lstrip()


def _wrap_lines_with_prefix(
    text: str,
    line_width: int,
    first_line_prefix_len: int,
    trailing_line_indent: int = 0,
) -> str:
    text_ = re.sub(r"\s+", " ", text).strip()
    first_line = textwrap.shorten(text_, line_width - first_line_prefix_len, placeholder="")
    tail = text_[len(first_line) :].lstrip()
    if tail:
        tail_lines = textwrap.wrap(tail, line_width)
        if trailing_line_indent:
            prefix = " " * trailing_line_indent
            tail_lines = [prefix + line for line in tail_lines]

        return "\n".join([first_line, *tail_lines])
    else:
        return first_line


def constructor_template(
    type_: Union[Type, Callable],
    module_name: Optional[str] = None,
    sig: Optional[Signature] = None,
    exclude: Optional[List[str]] = None,
    type_params: Optional[List[str]] = None,
) -> str:
    module = module_name or type_.__module__
    name = type_.__name__
    if sig is None:
        if isinstance(type_, type):
            sig = signature(type_.__init__)  # type: ignore
            is_method = True
        else:
            sig = signature(type_)
            is_method = False
        params = list(sig.parameters)[1:] if is_method else list(sig.parameters)
    else:
        params = list(sig.parameters)
    exclude_ = exclude or []
    args = ",\n    ".join(f"{name}={{{name}}}" for name in params if name not in exclude_)
    type_params_ = f"[{', '.join(type_params)}]" if type_params else ""
    template = f"{module}.{name}{type_params_}(\n    {args},\n)"
    return template


def render_constructor(template: str, kwargs: Dict[str, Any], var_name: Optional[str] = None) -> str:
    kwarg_strs = {name: repr(value) for name, value in kwargs.items()}
    rendered = template.format(**kwarg_strs)
    return rendered if var_name is None else f"{var_name} = {rendered}"


class VarName(str):
    def __repr__(self):
        return self


BLOB_STORE_SPEC_TEMPLATE = constructor_template(
    metaschema.RemoteBlobStoreSpec, sig=signature(metaschema.RemoteBlobStoreSpec)
)


def render_blob_store_def(blob_store: metaschema.RemoteBlobStoreSpec, var_name: str) -> str:
    return render_constructor(BLOB_STORE_SPEC_TEMPLATE, kwargs=blob_store.dict(), var_name=var_name)
