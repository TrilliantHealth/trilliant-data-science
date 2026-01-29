import subprocess
from datetime import datetime

from thds.core.log import getLogger
from thds.mops.pure import MemoizingPicklingRunner, pipeline_id_mask, use_runner

from ...config import TEST_TMP_URI

logger = getLogger(__name__)

# Session-scoped timestamp so all tests in a session share the same pipeline_id prefix.
# This avoids creating many nearly-identical pipeline IDs when running multiple tests.
_SESSION_TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%dT%H%M%S")


def _subprocess_remote(args_list):
    logger.info(f"Invoking shim runner with args {args_list}")
    subprocess.run(["python", "-m", "thds.mops.pure.core.entry.main", *args_list])
    logger.info("Completed shim runner")


runner = MemoizingPicklingRunner(_subprocess_remote, TEST_TMP_URI)


def clear_cache():
    pass


def _adls_shim_with_pipeline_id(func):
    """Wrap use_runner with a default pipeline_id for test identification."""
    pipeline_id = f"mops-test/{_SESSION_TIMESTAMP}"
    return pipeline_id_mask(pipeline_id)(use_runner(runner)(func))


adls_shim = _adls_shim_with_pipeline_id
