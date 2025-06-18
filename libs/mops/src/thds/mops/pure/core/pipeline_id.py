# This file must not import anything else from `remote.core` - it is a 'leaf' of our tree
# because it is depended upon by so many other things.
import os
import typing as ty
from contextlib import contextmanager
from datetime import datetime

from thds.core import hostname, log, meta, stack_context
from thds.termtool.colorize import colorized

# this is a global instead of a StackContext because we _do_ want it
# to spill over automatically into new threads.
_PIPELINE_ID = ""
logger = log.getLogger(__name__)


def __set_or_generate_pipeline_id_if_empty() -> None:
    some_unique_name = meta.get_repo_name() or os.getenv("THDS_DOCKER_IMAGE_NAME") or ""
    clean_commit = meta.get_commit()[:7] if meta.is_clean() else ""
    named_clean_commit = (
        f"{some_unique_name}/{clean_commit}" if some_unique_name and clean_commit else ""
    )

    def gen_pipeline_id() -> str:
        pipeline_id = (
            hostname.friendly()  # host name can be a group/directory now
            + "/"
            + "-".join(
                [
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    f"p{os.getpid()}",
                ]
            )
        )
        logger.warning(
            colorized(fg="black", bg="yellow")(f"Generated pipeline id '{pipeline_id}' for this run")
        )
        return pipeline_id

    set_pipeline_id(named_clean_commit or gen_pipeline_id())


def get_pipeline_id() -> str:
    """This will return the stack-local pipeline id, if set, or, if
    that is not set, will generate a global pipeline id and return
    that.

    Once a global pipeline id is generated, it will not be
    regenerated, although it can be overridden as a global with
    set_pipeline_id, and overridden for the stack with
    """
    if not _PIPELINE_ID:
        __set_or_generate_pipeline_id_if_empty()
    assert _PIPELINE_ID
    return _PIPELINE_ID


def set_pipeline_id(new_pipeline_id: str) -> None:
    """Override the current global pipeline id."""
    if not new_pipeline_id:
        return  # quietly disallow empty strings, since we always want a value here.
    global _PIPELINE_ID
    _PIPELINE_ID = new_pipeline_id


_STACK_LOCAL_PIPELINE_ID = stack_context.StackContext("STACK_LOCAL_PIPELINE_ID", "")


@contextmanager
def set_pipeline_id_for_stack(new_pipeline_id: str) -> ty.Iterator[str]:
    with _STACK_LOCAL_PIPELINE_ID.set(new_pipeline_id):
        yield new_pipeline_id


def get_pipeline_id_for_stack() -> str:
    return _STACK_LOCAL_PIPELINE_ID() or get_pipeline_id()
