"""Public API for masking the mops pipeline id.
"""

import re
import typing as ty
from contextlib import contextmanager
from functools import lru_cache

from thds.core.stack_context import StackContext

from .pipeline_id import get_pipeline_id_for_stack

_PIPELINE_ID_MASK = StackContext("PIPELINE_ID_MASK", "")


def get_pipeline_id_mask() -> str:
    """Returns the 'current' pipeline id, preferring:
    - the mask
    - the stack local
    - the global
    """
    return _PIPELINE_ID_MASK() or get_pipeline_id_for_stack()


@contextmanager
def pipeline_id_mask(pipeline_id: str) -> ty.Iterator[bool]:
    """Sets the pipeline id, but if it's already set, then the outer
    mask will take precedence over this one, i.e. this will be a
    no-op.

    Is a decorator as well as a ContextManager, thanks to the magic of
    @contextmanager. 🤯

    When used as a Context Manager, be aware that it will not be
    applied to threads launched by the current thread. To cross thread
    boundaries, prefer decorating an actual function that will then be
    launched in the thread.

    When used as a context manager, return True if this is the
    outermost layer and will actually be applied to the function;
    return False if not.

    The outermost configuration on this particular thread/green thread
    stack will be used. This pattern is very useful for libraries that
    want to define a default pipeline_id for their
    use_runner-decorated function.

    """
    if _PIPELINE_ID_MASK():
        yield False
    else:
        with _PIPELINE_ID_MASK.set(pipeline_id):
            yield True


F = ty.TypeVar("F", bound=ty.Callable)
_DOCSTRING_MASK_RE = re.compile(r".*pipeline-id(?:-mask)?:\s*(?P<pipeline_id>[^\s]+)\b", re.DOTALL)
# for backward-compatibility, we support pipeline-id-mask, even though the clearer name is
# ultimately pipeline-id.


@lru_cache(maxsize=32)
def extract_from_docstr(func: F, require: bool = True) -> str:
    if not func.__doc__:
        if not require:
            return ""
        raise ValueError(f"Function {func} must have a non-empty docstring to extract pipeline-id")
    m = _DOCSTRING_MASK_RE.match(func.__doc__)
    if not m:
        if "pipeline-id:" in func.__doc__ or "pipeline-id-mask:" in func.__doc__:
            raise ValueError("pipeline-id is present but empty - this is probably an accident")
        if not require:
            return ""
        raise ValueError(f"Cannot extract pipeline-id from docstring for {func}")
    mask = m.group("pipeline_id")
    assert mask, "pipeline-id should not have matched if it is empty"
    return mask


@contextmanager
def including_function_docstr(f: F) -> ty.Iterator[str]:
    with pipeline_id_mask(extract_from_docstr(f, require=False)):
        yield get_pipeline_id_mask()
