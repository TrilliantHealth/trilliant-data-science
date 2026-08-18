import os
import re
from datetime import datetime
from pathlib import Path

import pytest

from thds.core import source
from thds.mllegos import io


def test_round_trip_returns_a_source(tmp_path: Path) -> None:
    src = io.to_pickle_source({"a": 1}, "stuff", out_dir=tmp_path)
    assert isinstance(src, source.Source)
    assert io.load_pickle_source(src) == {"a": 1}


def test_typed_round_trip() -> None:
    src = io.to_pickle_source([1, 2, 3], "a_list")
    assert io.load_pickle_source_typed(list, src=src) == [1, 2, 3]


def test_typed_load_rejects_wrong_type() -> None:
    src = io.to_pickle_source([1, 2, 3], "a_list")
    with pytest.raises(TypeError, match="Expected"):
        io.load_pickle_source_typed(dict, src=src)


def test_typed_load_accepts_multiple_types() -> None:
    src = io.to_pickle_source(42, "an_int")
    assert io.load_pickle_source_typed(str, int, src=src) == 42
    with pytest.raises(TypeError, match="Expected"):
        io.load_pickle_source_typed(str, float, src=src)


def test_typed_load_requires_at_least_one_type() -> None:
    src = io.to_pickle_source(42, "an_int")
    with pytest.raises(TypeError, match="at least one type"):
        io.load_pickle_source_typed(src=src)


def test_out_dir_and_filename_pattern(tmp_path: Path) -> None:
    out_dir = tmp_path / "nested" / "out"  # created on demand
    src = io.to_pickle_source("data", "my_model", out_dir=out_dir)
    fpath = Path(os.fspath(src))
    assert fpath.parent == out_dir
    assert re.fullmatch(r"my_model_\d{8}_\d{6}_\d{6}\.pkl", fpath.name)


def test_default_out_dir_is_a_fresh_temp_dir() -> None:
    a = io.to_pickle_source("data", "same_stem")
    b = io.to_pickle_source("data", "same_stem")
    assert Path(os.fspath(a)).parent != Path(os.fspath(b)).parent


def test_same_microsecond_collision_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class _FrozenDatetime:
        @staticmethod
        def now() -> datetime:
            return datetime(2026, 1, 1, 12, 0, 0, 123456)

    monkeypatch.setattr(io, "datetime", _FrozenDatetime)
    io.to_pickle_source("data", "stem", out_dir=tmp_path)
    with pytest.raises(FileExistsError):
        io.to_pickle_source("data", "stem", out_dir=tmp_path)
