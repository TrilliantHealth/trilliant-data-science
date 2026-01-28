## utilities for providing remote context for naming things uniquely:
import typing as ty
from contextlib import contextmanager
from os import PathLike

from thds.core import uri_assign
from thds.core.stack_context import StackContext

from . import types, uris
from .memo import function_memospace

MEMO_URI_COMPONENTS = StackContext[ty.Optional[function_memospace.MemoUriComponents]](
    "MemoUriComponents", default=None
)
INVOCATION_RUN_ID = StackContext[str]("InvocationRunId", default="")


def invocation_output_uri(storage_root: uris.UriIsh = "", name: str = "") -> str:
    """If your function only outputs a single blob, you can safely use this without
    providing a name.  However, if you have multiple outputs from the same invocation, you
    must provide a meaningful name for each one.

    Example output path structure::

        file://~/.mops/mops2-mpf/my-pipeline/mymodule--myfunc/GhostJamLayer.abc123/2601271523-SkirtBus/output.parquet
        |____________| |_______| |_________| |______________| |__________________| |_________________| |____________|
         storage_root  mops2-mpf pipeline_id module--function   args_kwargs_hash         run_id            name

    The run_id ensures that concurrent executions of the same function with the same
    arguments write to separate output paths, preventing race conditions where one
    execution overwrites another's outputs.
    """
    storage_root = str(storage_root or uris.ACTIVE_STORAGE_ROOT())
    memo_uri_components = MEMO_URI_COMPONENTS()
    if not memo_uri_components:
        raise types.NotARunnerContext(
            "`invocation_output_uri` must be used in a `thds.mops.pure` runner context."
        )

    run_id = INVOCATION_RUN_ID()
    pipeline_function_key, function_arguments_key = memo_uri_components.invocation_unique_key()
    return uris.lookup_blob_store(storage_root).join(
        storage_root,
        pipeline_function_key,
        "--".join(filter(None, [name, function_arguments_key])),
        run_id,
        name,
        # we use the name twice now, so that the final part of the path also has a file extension
    )


def mops_uri_assignment(pathlike: ty.Union[str, PathLike]) -> str:
    # uses the newer core URI assignment logic, which includes path from current working
    # directory where possible.
    return uri_assign.replace_working_dirs_with_prefix(invocation_output_uri(), pathlike)


@contextmanager
def uri_assignment_context(
    memo_uri: str, runner_prefix: str = "", invocation_run_id: str = ""
) -> ty.Iterator[function_memospace.MemoUriComponents]:
    """Context manager to add mops2 URI assignment hook.

    The invocation_run_id, when provided, ensures outputs are written to unique paths
    per execution, preventing race conditions in concurrent runs.
    """
    memo_uri_components = function_memospace.parse_memo_uri(memo_uri, runner_prefix)
    with (
        MEMO_URI_COMPONENTS.set(memo_uri_components),
        INVOCATION_RUN_ID.set(invocation_run_id),
        uri_assign.add_hook(mops_uri_assignment),
    ):
        yield memo_uri_components
