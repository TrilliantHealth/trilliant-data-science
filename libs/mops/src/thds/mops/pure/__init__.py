# This module is the supported interface and everything not exported here is subject to change.
#
# The single exception is the joblib module, which is not exported by default
# to avoid requiring the additional dependency.

from . import adls  # noqa
from .core.entry import register_entry_handler
from .core.memo import results  # noqa
from .core.memo.function_memospace import (  # noqa
    add_pipeline_memospace_handlers,
    matching_mask_pipeline_id,
)
from .core.memo.results import require_all as require_all_results  # noqa
from .core.pipeline_id import get_pipeline_id, set_pipeline_id  # noqa
from .core.pipeline_id_mask import pipeline_id_mask, pipeline_id_mask_from_docstr  # noqa
from .core.source import create_source_at_uri  # noqa
from .core.types import Args, Kwargs, Runner  # noqa
from .core.uris import UriIsh, UriResolvable, register_blob_store  # noqa
from .core.use_runner import use_runner  # noqa
from .pickling.memoize_only import memoize_in  # noqa
from .pickling.mprunner import MemoizingPicklingRunner  # noqa
from .runner import Shell, ShellBuilder  # noqa


def _register_things():
    from . import pickling
    from .core.uris import load_plugin_blobstores

    register_entry_handler(
        pickling.mprunner.RUNNER_NAME,
        pickling.remote.run_pickled_invocation,  # type: ignore
    )

    load_plugin_blobstores()


_register_things()
