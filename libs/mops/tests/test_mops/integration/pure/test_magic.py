from datetime import datetime
from pathlib import Path

import pytest

from thds.mops import pure
from thds.mops.pure.runner.simple_shims import subprocess_shim

from ...config import TEST_TMP_URI


@pytest.fixture
def clear_magic():
    pure._magic._MAGIC_CONFIG = pure._magic._MagicConfig()  # type: ignore
    yield
    pure._magic._MAGIC_CONFIG = pure._magic._MagicConfig()  # type: ignore


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=f"test/pure-magic/{datetime.utcnow().isoformat()}")
def func1(a: int, b: str):
    return a * b


def test_pure_magic(clear_magic):
    assert func1(2, "a") == "aa"


_THIS_DIR = Path(__file__).parent


def test_load_magic_config(clear_magic):
    config_file = (_THIS_DIR / ".test.mops.toml").resolve()
    assert config_file.is_file()

    pure.magic.load_config_file(config_file)

    print()
    print("config before", pure._magic._get_config())
    print()

    @pure.magic()
    def func_test_config():
        pass

    print()
    print("config after", func_test_config.config)
    print()
    assert func_test_config._get_blob_root() == "adls://magicians/gob"
    assert func_test_config._pipeline_id == "final-countdown"
    assert subprocess_shim is func_test_config._shimbuilder(func_test_config, tuple(), dict())  # type: ignore
