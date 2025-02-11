# This module is the supported interface and everything not exported here is subject to change.
#
# The single exception is the joblib module, which is not exported by default
# to avoid requiring the additional dependency.

from . import adls  # noqa
from . import pickling
from .core import get_pipeline_id, set_pipeline_id, use_runner  # noqa
from .core.entry import register_entry_handler
from .core.memo import results  # noqa
from .core.memo.function_memospace import (  # noqa
    add_pipeline_memospace_handlers,
    matching_mask_pipeline_id,
)
from .core.memo.results import require_all as require_all_results  # noqa
from .core.pipeline_id_mask import pipeline_id_mask, pipeline_id_mask_from_docstr  # noqa
from .core.source import create_source_at_uri  # noqa
from .core.types import Args, Kwargs, Runner  # noqa
from .core.uris import UriIsh, UriResolvable  # noqa
from .pickling.memoize_only import memoize_in  # noqa
from .pickling.mprunner import MemoizingPicklingRunner  # noqa
from .runner import Shell, ShellBuilder  # noqa

register_entry_handler(
    pickling.mprunner.RUNNER_NAME,
    pickling.remote.run_pickled_invocation,  # type: ignore
)
