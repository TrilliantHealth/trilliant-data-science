import numpy as np
import pandas as pd
import pytest

pytest.importorskip("shap")
pytest.importorskip("sklearn")

from sklearn.ensemble import GradientBoostingClassifier  # noqa: E402

N_ROWS = 200
KEYS = ("customer_id", "region")
FEATURES = ("signal", "noise", "flag")


def _frame(seed: int, n_rows: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    signal = rng.normal(size=n_rows)
    return pd.DataFrame(
        {
            "customer_id": np.arange(n_rows) + 1_000,
            "region": [f"r{i % 7}" for i in range(n_rows)],
            "signal": signal,
            "noise": rng.normal(size=n_rows),
            "flag": pd.Series(rng.random(n_rows) > 0.5).astype("boolean"),
            "label": signal + 0.1 * rng.normal(size=n_rows) > 0,
        }
    )


@pytest.fixture(scope="module")
def frame() -> pd.DataFrame:
    """Keyed rows: two key columns, three features (one nullable boolean), one label."""
    return _frame(seed=0, n_rows=N_ROWS)


@pytest.fixture(scope="module")
def model(frame: pd.DataFrame) -> GradientBoostingClassifier:
    """A single-output binary classifier (`decision_function` is its raw log-odds)."""
    X = frame[list(FEATURES)].to_numpy(dtype=np.float32)
    return GradientBoostingClassifier(n_estimators=20, max_depth=2, random_state=0).fit(
        X, frame["label"]
    )
