# things that need to be configured for testing to work.
from pathlib import Path

from thds.adls import defaults

TEST_TMP_URI = defaults.mops_root() or "file://" + str(
    (Path(__file__).parents[2] / "mops-integration-tests").resolve()
)
TEST_DATA_TMP_URI = TEST_TMP_URI + "test/mops/"
