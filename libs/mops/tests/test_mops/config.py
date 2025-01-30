# things that need to be configured for testing to work.
from thds.core import config

_TEST_TMP_URI = config.item("test_tmp_uri", "adls://thdsscratch/tmp/")
TEST_TMP_URI = _TEST_TMP_URI()
TEST_DATA_TMP_URI = TEST_TMP_URI + "test/mops/"
