from collections.abc import Iterator

import pandas as pd
import pytest

pytest.importorskip("shap")
pytest.importorskip("sklearn")
matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")

from matplotlib import pyplot as plt  # noqa: E402
from matplotlib.figure import Figure  # noqa: E402
from sklearn.ensemble import GradientBoostingClassifier  # noqa: E402

from thds.mllegos import shaplegos  # noqa: E402

from .conftest import FEATURES, KEYS  # noqa: E402


@pytest.fixture(scope="module")
def keyed(model: GradientBoostingClassifier, frame: pd.DataFrame) -> shaplegos.KeyedExplanation:
    return shaplegos.explain_tree(model, frame.iloc[:50], feature_columns=FEATURES, key_columns=KEYS)


@pytest.fixture(autouse=True)
def _close_figures() -> Iterator[None]:
    yield
    plt.close("all")


def test_beeswarm_returns_figure(keyed: shaplegos.KeyedExplanation) -> None:
    fig = shaplegos.plots.beeswarm(keyed, max_display=2)
    assert isinstance(fig, Figure)
    assert len(fig.axes) >= 1


def test_violin_returns_figure(keyed: shaplegos.KeyedExplanation) -> None:
    fig = shaplegos.plots.violin(keyed, max_display=3)
    assert isinstance(fig, Figure)


def test_bar_returns_figure(keyed: shaplegos.KeyedExplanation) -> None:
    fig = shaplegos.plots.bar(keyed, max_display=3)
    assert isinstance(fig, Figure)
    labels = [tick.get_text() for tick in fig.axes[0].get_yticklabels()]
    assert any("signal" in label for label in labels)


def test_waterfall_returns_titled_figure(keyed: shaplegos.KeyedExplanation) -> None:
    customer_id, region = keyed.keys[4]
    fig = shaplegos.plots.waterfall(
        keyed.row(customer_id=customer_id, region=region),
        max_display=3,
        title=f"customer_id={customer_id}",
    )
    assert isinstance(fig, Figure)
    assert str(customer_id) in fig.get_suptitle()


def test_each_call_gets_its_own_figure_by_default(keyed: shaplegos.KeyedExplanation) -> None:
    first = shaplegos.plots.bar(keyed, max_display=2)
    second = shaplegos.plots.bar(keyed, max_display=2)
    assert first is not second


def test_plots_draw_on_a_given_axes(keyed: shaplegos.KeyedExplanation) -> None:
    given, ax = plt.subplots()
    assert shaplegos.plots.beeswarm(keyed, max_display=2, fig=ax) is given
    assert ax.collections  # the swarm landed on the caller's axes, not a new one
    assert shaplegos.plots.bar(keyed, max_display=2, fig=ax) is given


def test_plots_draw_on_a_given_figure(keyed: shaplegos.KeyedExplanation) -> None:
    given = plt.figure()
    assert shaplegos.plots.violin(keyed, max_display=2, fig=given) is given
    assert given.axes
    row = keyed.row(**dict(zip(keyed.key_names, keyed.keys[0])))
    other = plt.figure()
    assert shaplegos.plots.waterfall(row, max_display=2, fig=other) is other
    assert other.axes
