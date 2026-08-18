import pytest

pytest.importorskip("sklearn")

from sklearn.preprocessing import StandardScaler  # noqa: E402

from thds.mllegos import io  # noqa: E402
from thds.mllegos.sklegos import io as sk_io  # noqa: E402


def test_dump_and_load_model_round_trip() -> None:
    src = sk_io.dump_model(StandardScaler(with_mean=False))
    model = sk_io.load_model(src)
    assert isinstance(model, StandardScaler)
    assert model.with_mean is False


def test_load_model_rejects_non_estimator() -> None:
    src = io.to_pickle_source({"not": "an estimator"}, "not_a_model")
    with pytest.raises(TypeError, match="Expected"):
        sk_io.load_model(src)
