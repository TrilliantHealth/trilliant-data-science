import itertools
import os
import time
from typing import List, Mapping, NamedTuple, Tuple

import numpy as np
import pandas as pd
import pytest
from sklearn.base import BaseEstimator
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

from thds.mllegos.sklegos.modeling import tree_structured_labels as tsl
from thds.mllegos.util.tree import Trie

SCF = tsl.SubtreeClassifierSpec

SIMPLE_FLAT: tsl.ClassTree[str] = ["one", "two", "three"]
SIMPLE_NESTED_ONE_LEVEL: tsl.ClassTree[str] = ["one", ["two", "three"]]
SIMPLE_COUNTS = dict(one=1, two=2, three=3)

COMPLEX_FLAT: tsl.ClassTree[str] = [
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
]
COMPLEX_NESTED_ONE_LEVEL_WITH_SINGLETONS: tsl.ClassTree[str] = [
    ["one", "two", "three"],
    ["four", "five"],
    "six",
    ["seven"],
    ["eight", "nine"],
    [["ten"]],
    "eleven",
    [["twelve"], "thirteen"],
]
COMPLEX_NESTED_ONE_LEVEL: tsl.ClassTree[str] = [
    ["one", "two", "three"],
    ["four", "five"],
    "six",
    "seven",
    ["eight", "nine"],
]
COMPLEX_DEEPLY_NESTED = [
    ["one", ["two", "three"], ["thirty"]],
    [["four", [["five"], "six"]], "seven", "twenty"],
    "eight",
    ["nine", [["ten"], "eleven"]],
    "twelve",
]
COMPLEX_DEEPLY_NESTED_FILTERED = [
    ["one", ["two", "three"]],
    [["four", ["five", "six"]], "seven"],
    "eight",
    "nine",
]
COMPLEX_DEEPLY_NESTED_LIFTED = [
    "one",
    "two",
    "three",
    [["four", "five", "six"], "seven"],
    "eight",
    "nine",
]
COMPLEX_COUNTS = dict(one=1, two=2, three=3, four=4, five=5, six=6, seven=7, eight=8, nine=9)


@pytest.mark.parametrize(
    "tree, obs_counts, min_freq, expected",
    [
        pytest.param(SIMPLE_FLAT, SIMPLE_COUNTS, 1, SIMPLE_FLAT),
        pytest.param(SIMPLE_FLAT, SIMPLE_COUNTS, 10, SIMPLE_FLAT),
        pytest.param(SIMPLE_NESTED_ONE_LEVEL, SIMPLE_COUNTS, 1, SIMPLE_NESTED_ONE_LEVEL),
        pytest.param(SIMPLE_NESTED_ONE_LEVEL, SIMPLE_COUNTS, 10, SIMPLE_FLAT),
        # complex
        pytest.param(
            COMPLEX_NESTED_ONE_LEVEL_WITH_SINGLETONS, COMPLEX_COUNTS, 3, COMPLEX_NESTED_ONE_LEVEL
        ),
        pytest.param(COMPLEX_NESTED_ONE_LEVEL_WITH_SINGLETONS, COMPLEX_COUNTS, 20, COMPLEX_FLAT),
        pytest.param(COMPLEX_DEEPLY_NESTED, COMPLEX_COUNTS, 1, COMPLEX_DEEPLY_NESTED_FILTERED),
        pytest.param(COMPLEX_DEEPLY_NESTED, COMPLEX_COUNTS, 15, COMPLEX_DEEPLY_NESTED_LIFTED),
        pytest.param(COMPLEX_DEEPLY_NESTED, COMPLEX_COUNTS, 25, COMPLEX_FLAT),
    ],
)
def test__lift(
    tree: tsl.ClassTree[str], obs_counts: Mapping[str, int], min_freq: int, expected: tsl.ClassTree[str]
):
    lifted = tsl._lift(tree, obs_counts, min_freq)
    assert lifted == expected


@pytest.mark.parametrize(
    "tree, obs_counts, min_freq, expected",
    [
        pytest.param(
            SIMPLE_FLAT,
            SIMPLE_COUNTS,
            1,
            Trie(
                SCF(
                    dict(one=0, two=1, three=2),
                    [(0, 1), (1, 2), (2, 3)],
                    6,
                ),
                {},
            ),
            id="simple",
        ),
        pytest.param(
            SIMPLE_NESTED_ONE_LEVEL,
            SIMPLE_COUNTS,
            1,
            Trie(
                SCF(
                    dict(one=0, two=1, three=1),
                    [(0, 1), (1, 3)],
                    6,
                ),
                {
                    1: Trie(
                        SCF(
                            dict(two=0, three=1),
                            [(1, 2), (2, 3)],
                            5,
                        ),
                        {},
                    ),
                },
            ),
            id="simple nested",
        ),
        pytest.param(
            SIMPLE_NESTED_ONE_LEVEL,
            SIMPLE_COUNTS,
            6,
            Trie(
                SCF(
                    dict(one=0, two=1, three=2),
                    [(0, 1), (1, 2), (2, 3)],
                    6,
                ),
                {},
            ),
            id="simple nested with threshold",
        ),
        pytest.param(
            COMPLEX_NESTED_ONE_LEVEL_WITH_SINGLETONS,
            COMPLEX_COUNTS,
            3,
            Trie(
                SCF(
                    dict(one=0, two=0, three=0, four=1, five=1, six=2, seven=3, eight=4, nine=4),
                    [(0, 3), (3, 5), (5, 6), (6, 7), (7, 9)],
                    45,
                ),
                {
                    0: Trie(
                        SCF(
                            dict(one=0, two=1, three=2),
                            [(0, 1), (1, 2), (2, 3)],
                            6,
                        ),
                        {},
                    ),
                    1: Trie(
                        SCF(
                            dict(four=0, five=1),
                            [(3, 4), (4, 5)],
                            9,
                        ),
                        {},
                    ),
                    4: Trie(
                        SCF(
                            dict(eight=0, nine=1),
                            [(7, 8), (8, 9)],
                            17,
                        ),
                        {},
                    ),
                },
            ),
            id="complex nested filtered",
        ),
        pytest.param(
            COMPLEX_DEEPLY_NESTED,
            COMPLEX_COUNTS,
            15,
            Trie(
                SCF(
                    dict(one=0, two=1, three=2, four=3, five=3, six=3, seven=3, eight=4, nine=5),
                    [(0, 1), (1, 2), (2, 3), (3, 7), (7, 8), (8, 9)],
                    45,
                ),
                {
                    3: Trie(
                        SCF(
                            dict(four=0, five=0, six=0, seven=1),
                            [(3, 6), (6, 7)],
                            22,
                        ),
                        {
                            0: Trie(
                                SCF(
                                    dict(four=0, five=1, six=2),
                                    [(3, 4), (4, 5), (5, 6)],
                                    15,
                                ),
                                {},
                            ),
                        },
                    ),
                },
            ),
        ),
    ],
)
def test_generate_class_tree(
    tree: tsl.ClassTree[str],
    obs_counts: Mapping[str, int],
    min_freq: int,
    expected: Trie[int, tsl.SubtreeClassifierSpec],
):
    trie, classes = tsl.generate_class_tree(tree, pd.Series(obs_counts), min_freq)
    classes_ = [
        (c, ix)
        for subtree in (t.tree for t in trie.dfs())
        for c, i in subtree.value.class_mapping.items()
        if (subtree.value.class_ranges[i][1] - (ix := subtree.value.class_ranges[i][0])) == 1
    ]
    classes_.sort(key=lambda c_ix: c_ix[1])
    assert [
        c for c, ix in classes_
    ] == classes  # ranges of width 1 should correspond to the class labels
    assert all(
        i == ix for i, ix in enumerate(ix for _, ix in classes_)
    )  # their indices should be contiguous
    # and cover the entire range [0, #classes)
    assert len(classes) == len(set(classes))  # classes should be unique
    assert set(classes) == set(obs_counts)  # all observed classes should be in the tree
    assert trie == expected

    def _test_properties(trie: Trie[int, tsl.SubtreeClassifierSpec]):
        value = trie.value
        assert all(
            next_start == last_end
            for (_, last_end), (next_start, _) in zip(value.class_ranges, value.class_ranges[1:])
        )
        for i, (start, end) in enumerate(value.class_ranges):
            classes = {c for c, ix in value.class_mapping.items() if ix == i}
            if end - start == 1:
                # terminal class node; no subtree
                assert i not in trie.children
            else:
                child = trie.children[i]
                assert child.value.range == (start, end)
                assert set(child.value.class_mapping) == classes

    assert trie.value.range == (0, len(obs_counts))
    for subtrie in trie.dfs():
        _test_properties(subtrie.tree)


def generate_data(
    branching_factor: int, leaf_sample_size: int, depth: int, seed: int, scale: float = 0.2
):
    """Generate a dataset of labeled clusters structured in a self-similar hierarchy. At base, each class
    corresponds to a Gaussian blob in 2 dimensions containing `leaf_sample_size` points, and those arrange
    into rings of `branching_factor` clusters, which further arrange into rings of `branching_factor`
    clusters, etc, recursively to `depth` levels, with the center of the outermost level at the origin and
    the blobs one level below being centered on the unit disc. The integer classes correspond to an "address"
    in the hierarchy, with digits to base `branching_factor` specifying the location at each level, starting
    from 0 at 3 o'clock and counting upwards counter-clockwise. Quadratic features are included in the final
    feature array, since in theory these should be sufficient to cleanly separate each class. The final
    dataset has `leaf_sample_size * branching_factor ** depth` samples with `branching_factor ** depth`
    unique class labels."""
    np.random.seed(seed)
    # Total number of classes:
    n_classes = branching_factor**depth
    # Total number of samples:
    n = leaf_sample_size * n_classes
    # Gaussian data centered at the origin, with variance scaled down according to depth of the tree
    dataset = np.random.normal(scale=scale**depth, size=(n, 2))
    # The class labels, as ints
    classes = np.arange(n_classes)
    # ... and paths in the tree
    addresses = np.array(list(itertools.product(*([range(branching_factor)] * depth))))
    # Centroids to add to the initial Gaussian data:
    angles = (2 * np.pi / branching_factor) * addresses
    scales = scale ** np.arange(depth)
    xs = (np.cos(angles) * scales).sum(axis=1)
    ys = (np.sin(angles) * scales).sum(axis=1)
    centers = np.array([xs, ys]).T
    # Shift the Gaussian data according to the centroids
    dataset += np.repeat(centers, leaf_sample_size, axis=0)
    # Aligned array of class labels matching the centroids at each row
    labels = np.repeat(classes, leaf_sample_size)
    # Construct nested class tree; e.g. for branching factor 2, depth 3 this would be [[[0, 1], [2, 3]], [[4, 5], [6, 7]]]
    class_tree = classes.reshape([branching_factor] * depth).tolist()
    return dataset, labels, class_tree


@pytest.fixture(scope="module")
def perf_multiplier():
    return 40 if os.getenv("CI") else 5


def _run_time(n_trials: int, func, *args, **kwargs):
    def inner():
        tic = time.perf_counter()
        result = func(*args, **kwargs)
        runtime = time.perf_counter() - tic
        return result, runtime

    results = [inner() for _ in range(max(n_trials, 1))]
    return results[0][0], max(t for _, t in results)


def _assert_correct_shape_and_dtype(
    X: np.ndarray | pd.DataFrame, probs: pd.DataFrame, clf: tsl.TreeStructuredLabelsClassifier
):
    assert probs.shape[0] == X.shape[0]
    assert probs.shape[1] == len(clf.classes_)
    assert (probs.dtypes.apply(lambda dt: dt.name) == clf.prob_dtype).all()


def _assert_normalized(probs: pd.DataFrame | np.ndarray):
    sums = np.sum(probs, axis=1)
    assert np.allclose(sums, 1.0)


class SampleDataParams(NamedTuple):
    branching_factor: int
    depth: int
    leaf_sample_size: int

    @property
    def n_classes(self):
        return self.branching_factor**self.depth


class SampleData(NamedTuple):
    X: np.ndarray
    y: np.ndarray
    test_X: np.ndarray
    test_y: np.ndarray
    class_tree: tsl.ClassTree[int]
    params: SampleDataParams


def _sample_data_param(branching_factor: int, depth: int, leaf_sample_size: int):
    return pytest.param(
        SampleDataParams(branching_factor, depth, leaf_sample_size),
        id=f"b={branching_factor}_d={depth}_lss={leaf_sample_size}",
    )


@pytest.fixture(
    scope="session",
    params=[
        _sample_data_param(5, 2, 200),
        _sample_data_param(2, 3, 100),
        _sample_data_param(3, 3, 100),
        _sample_data_param(4, 3, 100),
        _sample_data_param(5, 3, 100),
        _sample_data_param(2, 4, 50),
        _sample_data_param(3, 4, 50),
        _sample_data_param(4, 4, 50),
    ],
)
def sample_data(request) -> SampleData:
    sample_data_params: SampleDataParams = request.param
    branching_factor = sample_data_params.branching_factor
    depth = sample_data_params.depth
    leaf_sample_size = sample_data_params.leaf_sample_size
    X, y, class_tree = generate_data(branching_factor, leaf_sample_size, depth, seed=42)
    test_X, test_y, _ = generate_data(branching_factor, leaf_sample_size // 2, depth, seed=1729)
    print(
        f"branching: {branching_factor}, depth: {depth}, #classes: {len(np.unique(y))}, "
        f"#train: {len(X)}, #test: {len(test_X)}"
    )
    return SampleData(
        X=X,
        y=y,
        test_X=test_X,
        test_y=test_y,
        class_tree=class_tree,
        params=sample_data_params,
    )


class ClassifierStats(NamedTuple):
    clf: tsl.TreeStructuredLabelsClassifier | LogisticRegression
    fit_time: float


@pytest.fixture(scope="session")
def fit_naive_clf(sample_data) -> ClassifierStats:
    clf = LogisticRegression()
    clf, fit_time = _run_time(3, clf.fit, sample_data.X, sample_data.y)
    return ClassifierStats(clf=clf, fit_time=fit_time)


@pytest.fixture(scope="session")
def fit_tree_structured_clf(sample_data) -> ClassifierStats:
    clf = tsl.TreeStructuredLabelsClassifier(
        sample_data.class_tree,
        LogisticRegression(),
        max_rows_train=sample_data.X.shape[0] // sample_data.params.branching_factor,
        # ^ only train on a fraction of the data at the root that would equal the number of samples at each subtree
    )
    clf, fit_time = _run_time(3, clf.fit, sample_data.X, sample_data.y)
    return ClassifierStats(clf=clf, fit_time=fit_time)


SMOOTHING_PARAMS = [
    pytest.param(smoothing, id=f"smooth={smoothing}") for smoothing in [0.1, 0.25, 0.5, None]
]


@pytest.mark.parametrize(
    "smoothing",
    SMOOTHING_PARAMS,
)
def test_tree_structured_labels_probability_properties(
    sample_data: SampleData,
    fit_tree_structured_clf: ClassifierStats,
    smoothing: float | None,
):
    clf = fit_tree_structured_clf.clf
    X = sample_data.X
    clf.set_inference_params(temperature_smoothing=smoothing)
    assert clf.temperature_smoothing == smoothing

    def inner(pruning: bool):
        # Check that the predicted probabilities are valid
        prior_pruning = clf.pruning
        clf.set_inference_params(pruning=pruning)
        assert clf.pruning == pruning

        probs, pred_time = _run_time(3, clf.predict_proba, X)
        _assert_correct_shape_and_dtype(X, probs, clf)
        _assert_normalized(probs)
        # same for log-probabilities
        log_probs = clf.predict_log_proba(X)
        _assert_correct_shape_and_dtype(X, log_probs, clf)
        probs_from_log_probs = np.exp(log_probs)
        _assert_normalized(probs_from_log_probs)

        assert np.allclose(probs, probs_from_log_probs)
        classes = probs.idxmax(1)
        max_probs = probs.max(axis=1)
        clf.set_inference_params(pruning=prior_pruning)
        return probs, max_probs, classes, pred_time

    log_level = tsl._LOGGER.getEffectiveLevel()
    tsl._LOGGER.setLevel(0)  # show debug-level logs during inference to allow inspection with pytest -s
    probs, max_probs, classes, pred_time = inner(pruning=False)
    probs_pruned, max_probs_pruned, classes_pruned, pred_time_pruned = inner(pruning=True)
    clf.set_inference_params(pruning=False, temperature_smoothing=None)
    tsl._LOGGER.setLevel(log_level)
    print(
        f"\npredict time: {pred_time:.4f}s, predict time with pruning: {pred_time_pruned:.4f}s, "
        f"speedup: {pred_time / pred_time_pruned:.2f}x"
    )
    # pruning should not affect the final point prediction
    assert np.all(classes == classes_pruned)
    assert np.allclose(max_probs, max_probs_pruned)
    # but the pruned probabilities should be more smooth (i.e. higher entropy/gini impurity)
    assert not np.allclose(probs, probs_pruned)
    impurity = 1.0 - np.sum(np.square(probs), axis=1)
    impurity_pruned = 1.0 - np.sum(np.square(probs_pruned), axis=1)
    assert np.all(impurity <= impurity_pruned)
    if sample_data.params.n_classes > (100 if os.getenv("CI") else 20) and sample_data.params.depth > 2:
        assert pred_time_pruned < pred_time


@pytest.mark.parametrize(
    "smoothing",
    SMOOTHING_PARAMS,
)
def test_tree_structured_labels_beats_baseline(
    sample_data: SampleData,
    fit_naive_clf: ClassifierStats,
    fit_tree_structured_clf: ClassifierStats,
    perf_multiplier: int,
    smoothing: float | None,
):
    test_X = sample_data.test_X
    test_y = sample_data.test_y
    naive_clf = fit_naive_clf.clf
    naive_fit_time = fit_naive_clf.fit_time
    clf = fit_tree_structured_clf.clf
    fit_time = fit_tree_structured_clf.fit_time

    naive_preds, naive_pred_time = _run_time(3, naive_clf.predict, test_X)
    naive_acc = accuracy_score(test_y, naive_preds)

    clf.set_inference_params(temperature_smoothing=smoothing, pruning=True)
    assert clf.temperature_smoothing == smoothing
    assert clf.pruning
    preds, pred_time = _run_time(3, clf.predict, test_X)
    clf.set_inference_params(temperature_smoothing=None, pruning=False)
    acc = accuracy_score(test_y, preds)
    error_reduction = 1.0 - (1.0 - acc) / (1.0 - naive_acc)
    print(
        f"\nnaive accuracy: {naive_acc:.3f}, tree-structured model accuracy: {acc:.3f}, "
        f"error reduction: {error_reduction:.3f}"
    )
    print(
        f"naive fit time: {naive_fit_time:.4f}s, tree-structured model fit time: {fit_time:.4f}s, "
        f"speedup: {naive_fit_time / fit_time:.2f}x"
    )
    print(
        f"naive predict time: {naive_pred_time:.4f}s, tree-structured model predict time: {pred_time:.4f}s, "
        f"slowdown: {pred_time / naive_pred_time:.2f}x"
    )
    n_subtrees = sum(1 for t in clf.tree_.dfs())

    assert acc > naive_acc
    assert acc >= 0.995
    assert fit_time < (perf_multiplier * naive_fit_time)
    assert pred_time < (perf_multiplier * n_subtrees * naive_pred_time)
    if sample_data.params.depth > 2:
        assert error_reduction > 0.99
    else:
        assert error_reduction > 0.75


def test_tree_structured_labels_classifier_calibrated():
    class MarginalClassDistClassifier(BaseEstimator):
        def fit(self, X, y):
            self.probs_ = pd.Series(y).value_counts(normalize=True).sort_index()
            self.classes_ = self.probs_.index.tolist()
            return self

        def predict_proba(self, X):
            return np.tile(self.probs_.values, len(X)).reshape(len(X), len(self.probs_))

    class_tree = [1, 2, [3, [4, 5], 6], [7, [8, [9, 10]]], [11, 12], 13, [14, 15, 16]]
    n_classes = sum(1 for _ in tsl._leaves(class_tree))
    clf = tsl.TreeStructuredLabelsClassifier(
        class_tree,
        MarginalClassDistClassifier(),
    )
    X = np.zeros((n_classes, 1))  # dummy features; model ignores
    y = np.arange(1, n_classes + 1)  # uniformly distributed labels
    clf.fit(X, y)
    max_depth = max(len(t.path) for t in clf.tree_.dfs())
    assert max_depth == 3  # assert that nontrivial tree of classifiers was learned
    probs = clf.predict_proba(X)
    assert np.allclose(probs, 1 / n_classes)
    log_probs = clf.predict_log_proba(X)
    assert np.allclose(log_probs, -np.log(n_classes))
    # probabilities should be uniform even though they were estimated locally and computed as chained
    # conditional probabilities


@pytest.mark.parametrize(
    "paths, expected",
    [
        pytest.param(
            [
                (["one"], 0),
                (["two"], 1),
                (["three"], 2),
            ],
            [0, 2, 1],  # alphabetical order by keys
            id="simple",
        ),
        pytest.param(
            [(["one", "two", "three", "four"], 0)],
            [0],
            id="singleton",
        ),
        pytest.param(
            [
                (["one", "two", "three"], 0),
                (["one", "two", "four"], 1),
                (["one", "two", "five"], 2),
                (["one", "two", "six"], 3),
            ],
            [2, 1, 3, 0],
            id="fixed-length nested reducing to flat",
        ),
        pytest.param(
            [
                (["one", "two", "three"], 0),
                (["one", "two", "four"], 1),
                (["one", "two", "five"], 2),
                (["two", "one", "one"], 3),
                (["two", "one", "two"], 4),
                (["two", "three", "four"], 5),
                (["two", "three", "five"], 6),
                (["two", "three", "six"], 7),
            ],
            [[2, 1, 0], [[3, 4], [6, 5, 7]]],
            id="fixed-length nested",
        ),
        pytest.param(
            [
                (["one", "two", "three"], 0),
                (["one", "two", "four"], 1),
                (["one", "two", "five"], 2),
                (["two", "three"], 4),
                (["two", "four"], 5),
                (["three", "four"], 6),
                (["three", "five"], 7),
            ],
            [[2, 1, 0], [7, 6], [5, 4]],
            id="variable-length nested",
        ),
        pytest.param(
            [
                (["one", "two", "three"], 0),
                (["one", "two", "four"], 1),
                (["one", "two", "five"], 2),
                (["two", "three"], 4),
                (["two", "four"], 5),
                (["three", "four"], 6),
            ],
            [[2, 1, 0], 6, [5, 4]],
            id="variable-length nested with singleton",
        ),
        pytest.param(
            [
                (["one", "two", "three"], 0),
                (["one", "two", "four"], 1),
                (["one", "two", "five"], 2),
                (["two", "three"], 4),
                (["two", "four"], 5),
                (["three", "four"], 6),
            ],
            [[2, 1, 0], 6, [5, 4]],
            id="variable-length nested with singleton",
        ),
        pytest.param(
            [
                (["one", "two", "three"], 0),
                (["one", "two", "four"], 1),
                (["one", "two", "five"], 2),
                (["one", "two"], 7),  # intermediate label
                (["two", "three"], 4),
                (["two", "four"], 5),
                (["three", "four"], 6),
            ],
            [[7, 2, 1, 0], 6, [5, 4]],
            id="variable-length nested with singleton and intermediate label",
        ),
    ],
)
def test_class_tree_from_paths(paths: List[Tuple[List[str], int]], expected: tsl.ClassTree[int]):
    tree = tsl.class_tree_from_paths(paths)
    assert tree == expected


def test_subtree_executor_recording(sample_data):
    """A recording executor should see all subtree fit tasks with correct shapes."""
    recorded_tasks: list[tsl.SubtreeFitTask] = []

    def recording_executor(classifier, tasks):
        from sklearn.base import clone

        for task in tasks:
            recorded_tasks.append(task)
            fitted = clone(classifier).fit(
                task.X,
                task.y,
                **(dict(sample_weight=task.sample_weight) if task.sample_weight is not None else {}),
            )
            yield task.path, fitted

    clf = tsl.TreeStructuredLabelsClassifier(
        sample_data.class_tree,
        LogisticRegression(),
        subtree_executor=recording_executor,
    )
    clf.fit(sample_data.X, sample_data.y)

    # every internal node of the tree should have a task
    n_tree_nodes = sum(1 for _ in clf.tree_.dfs())
    assert len(recorded_tasks) == n_tree_nodes

    for task in recorded_tasks:
        assert isinstance(task.path, tuple)
        assert task.X.shape[0] == len(task.y), f"X and y row count mismatch at path {task.path}"
        assert task.X.shape[1] == sample_data.X.shape[1], "feature count should match input"

    # predictions should work normally after fitting via executor
    probs = clf.predict_proba(sample_data.test_X)
    _assert_correct_shape_and_dtype(sample_data.test_X, probs, clf)
    _assert_normalized(probs)


def test_subtree_executor_threaded_matches_sequential(sample_data):
    """A threaded executor should produce identical predictions to the default sequential one."""
    from concurrent.futures import ThreadPoolExecutor

    from sklearn.base import clone

    def threaded_executor(classifier, tasks):
        tasks_list = list(tasks)

        def _fit_one(task):
            fitted = clone(classifier).fit(
                task.X,
                task.y,
                **(dict(sample_weight=task.sample_weight) if task.sample_weight is not None else {}),
            )
            return task.path, fitted

        with ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(_fit_one, tasks_list))

    clf_seq = tsl.TreeStructuredLabelsClassifier(
        sample_data.class_tree,
        LogisticRegression(random_state=42),
        random_state=0,
    )
    clf_seq.fit(sample_data.X, sample_data.y)

    clf_par = tsl.TreeStructuredLabelsClassifier(
        sample_data.class_tree,
        LogisticRegression(random_state=42),
        random_state=0,
        subtree_executor=threaded_executor,
    )
    clf_par.fit(sample_data.X, sample_data.y)

    probs_seq = clf_seq.predict_proba(sample_data.test_X)
    probs_par = clf_par.predict_proba(sample_data.test_X)
    np.testing.assert_array_almost_equal(probs_seq.values, probs_par.values)
