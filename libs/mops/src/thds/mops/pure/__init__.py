# This module is the supported interface and everything not exported here is subject to change.
#
# The single exception is the joblib module, which is not exported by default
# to avoid requiring the additional dependency.

from . import adls  # noqa
from .core import get_pipeline_id, set_pipeline_id, use_runner  # noqa
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
from .pickling.runner import MemoizingPicklingRunner, Shell, ShellBuilder  # noqa

AdlsPickleRunner = MemoizingPicklingRunner  # an old alias
