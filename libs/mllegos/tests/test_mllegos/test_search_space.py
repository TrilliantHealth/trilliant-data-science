"""The range types must stay plain, hashable, and picklable - they are meant to be embedded in
configuration objects that serve as memoization keys."""

import pickle
import sys

import pytest

from thds.mllegos import search_space


def test_ranges_are_hashable_and_pickle_round_trip() -> None:
    rng = search_space.IntRange(1, 10)
    assert hash(rng) == hash(search_space.IntRange(1, 10))
    assert pickle.loads(pickle.dumps(rng)) == rng

    frng = search_space.FloatRange(1e-3, 1.0, log_uniform=True)
    assert hash(frng) == hash(search_space.FloatRange(1e-3, 1.0, log_uniform=True))
    assert pickle.loads(pickle.dumps(frng)) == frng
    assert frng != search_space.FloatRange(1e-3, 1.0)


def test_build_skopt_space_maps_the_ranges() -> None:
    skopt_space = pytest.importorskip("skopt.space")
    space = search_space.build_skopt_space(
        {
            "n_estimators": search_space.IntRange(150, 600),
            "learning_rate": search_space.FloatRange(1e-2, 3e-1, log_uniform=True),
            "subsample": search_space.FloatRange(0.6, 1.0),
        }
    )

    assert set(space) == {"n_estimators", "learning_rate", "subsample"}
    assert isinstance(space["n_estimators"], skopt_space.Integer)
    assert (space["n_estimators"].low, space["n_estimators"].high) == (150, 600)
    assert isinstance(space["learning_rate"], skopt_space.Real)
    assert space["learning_rate"].prior == "log-uniform"
    assert space["subsample"].prior == "uniform"


def test_build_skopt_space_rejects_unknown_range_types() -> None:
    """Unsupported value types must be rejected at space-building time, not deep inside a
    search run."""
    pytest.importorskip("skopt.space")
    with pytest.raises(TypeError, match="unsupported search-space dimension type: tuple"):
        search_space.build_skopt_space({"n_estimators": (150, 600)})  # type: ignore[dict-item]


def test_build_skopt_space_without_skopt_names_the_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "skopt", None)
    monkeypatch.delitem(sys.modules, "skopt.space", raising=False)
    with pytest.raises(ImportError, match=r"thds\.mllegos\[skopt\]"):
        search_space.build_skopt_space({"n_estimators": search_space.IntRange(1, 2)})
