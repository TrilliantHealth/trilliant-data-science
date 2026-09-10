import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
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


def test_parquet_round_trip(tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    src = io.to_parquet_source(df, "a_frame", out_dir=tmp_path)
    assert isinstance(src, source.Source)
    assert Path(os.fspath(src)).name == "a_frame.parquet"
    pd.testing.assert_frame_equal(pd.read_parquet(src), df)


def test_parquet_series_written_as_one_column_frame() -> None:
    series = pd.Series([1.0, 2.0], name="score")
    src = io.to_parquet_source(series, "a_series")
    round_tripped = pd.read_parquet(src)
    pd.testing.assert_frame_equal(round_tripped, series.to_frame())


def test_parquet_existing_file_raises(tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1]})
    io.to_parquet_source(df, "same_name", out_dir=tmp_path)
    with pytest.raises(FileExistsError):
        io.to_parquet_source(df, "same_name", out_dir=tmp_path)


def test_json_round_trip(tmp_path: Path) -> None:
    data = {"metric": 0.9, "labels": ["a", "b"]}
    src = io.to_json_source(data, "stats", out_dir=tmp_path)
    assert Path(os.fspath(src)).name == "stats.json"
    assert io.load_json_source(src) == data


def test_json_indent_is_applied(tmp_path: Path) -> None:
    src = io.to_json_source({"a": 1}, "pretty", out_dir=tmp_path, indent=2)
    assert Path(os.fspath(src)).read_text() == '{\n  "a": 1\n}'
