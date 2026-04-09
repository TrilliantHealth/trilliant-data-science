from dataclasses import dataclass
from itertools import chain
from operator import itemgetter
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin, TransformerMixin
from sklearn.preprocessing import normalize
from sklearn.utils import validation

from thds.core import parallel
from thds.core.log import getLogger

from ...util.tree import Trie, tree_acc, trie_from_counts, trie_from_values
from .. import array_compat
from .. import util as sklegos_util
from ..sampling import stratified
from ..types import Any1DArray, Any2DArray

C = TypeVar("C", bound=Hashable)
H = TypeVar("H", bound=Hashable)
ClassTree = List[Union[C, "ClassTree[C]"]]

_LOGGER = getLogger(__name__)


@dataclass
class SubtreeClassifierSpec(Generic[C]):
    class_mapping: Mapping[C, int]  # at training time, classes are mapped to these labels
    class_ranges: List[Tuple[int, int]]
    # after inference, the mapping is reversed to find the column ranges in the probability array to update.
    # the values of `class_mapping` are indexes in `class_ranges`
    count: int
    classifier: Optional[ClassifierMixin] = None

    @property
    def range(self) -> Tuple[int, int]:
        return self.class_ranges[0][0], self.class_ranges[-1][1]


def class_tree_from_paths(paths: Iterable[Tuple[Sequence[H], C]]) -> ClassTree[C]:
    """Construct a tree of class labels from an iterator of variable-length paths to each class label.
    The entries in the paths must be orderable and hashable."""
    trie: Trie[H, Optional[C]] = trie_from_values(paths, default=None)
    classes = [node.tree.value for node in trie.dfs() if node.tree.value is not None]
    assert len(classes) == len(set(classes)), "Duplicate class labels in tree"

    def _from_trie(trie: Trie[H, Optional[C]]) -> ClassTree[C]:
        prefix: ClassTree[C] = [] if trie.value is None else [trie.value]
        if not trie.children:
            return prefix
        elif len(trie.children) == 1:
            child = next(iter(trie.children.values()))
            suffix = _from_trie(child)
        else:

            def flatten(node):
                return node[0] if len(node) == 1 else node

            suffix = [
                flatten(_from_trie(child))
                for _, child in sorted(trie.children.items(), key=itemgetter(0))
            ]
        return (prefix + suffix) if prefix else suffix

    return _from_trie(trie)


def paths(tree: ClassTree[C] | C, prefix: Tuple[int, ...] = ()) -> Iterable[Tuple[Tuple[int, ...], C]]:
    if isinstance(tree, list):
        return chain.from_iterable(paths(subtree, prefix + (i,)) for i, subtree in enumerate(tree))
    else:
        return [(prefix, tree)]


def _leaves(class_tree: ClassTree[C]) -> Iterator[C]:
    """Get the leaf nodes of a tree of class labels, in pre-order traversal."""
    return chain.from_iterable(
        _leaves(node) if isinstance(node, list) else [node] for node in class_tree
    )


def _prune(trie: Trie[int, SubtreeClassifierSpec]) -> Trie[int, SubtreeClassifierSpec]:
    """Remove internal nodes that won't correspond to a classifier"""
    to_prune = [ix for ix, child in trie.children.items() if not child.children]
    for ix in to_prune:
        trie.children.pop(ix)
    for subtree in trie.children.values():
        _prune(subtree)
    return trie


def _to_tree(class_tree: ClassTree[C], counts: Mapping[C, int]) -> Trie[int, SubtreeClassifierSpec]:
    """Convert a tree of class labels to a trie, where each node is a prefix of the class labels
    under it."""

    def accumulate_class_mappings(
        count: int, children: Mapping[int, Trie[int, SubtreeClassifierSpec]], path: Tuple[int, ...]
    ) -> SubtreeClassifierSpec:
        if children:
            subtrees = sorted(children.items(), key=itemgetter(0))
            cls_mapping = {cls: i for i, subtree in subtrees for cls in subtree.value.class_mapping}
            ranges = [subtree.value.range for _, subtree in subtrees]
            return SubtreeClassifierSpec(cls_mapping, ranges, count)
        else:
            return SubtreeClassifierSpec(
                {(cls := path_to_cls[path]): 0}, [((ix := class_ids[cls]), ix + 1)], counts[cls]
            )

    path_to_cls = dict(paths(class_tree))
    class_ids = {cls: i for i, cls in enumerate(_leaves(class_tree))}
    count_trie = trie_from_counts(
        ((p, counts[cls]) for p, cls in path_to_cls.items()), count_prefixes=True
    )
    return _prune(tree_acc(accumulate_class_mappings, count_trie))


def _lift(
    class_tree: ClassTree[C], obs_counts: Mapping[C, int], min_frequency: int, level: int = 0
) -> ClassTree[C]:
    """Remove internal nodes of the class tree with counts too low to train a classifier, or with only
    one branch so that a classifier is unnecessary"""
    leaves = iter(_leaves(class_tree))
    flattened: ClassTree[C] = []
    count = 0
    for leaf in leaves:
        if (c := obs_counts.get(leaf, 0)) > 0:
            flattened.append(leaf)
            count += c
        if count >= min_frequency:
            # enough support; don't flatten
            result = list(
                chain.from_iterable(
                    (
                        _lift(subtree, obs_counts, min_frequency, level + 1)
                        if isinstance(subtree, list)
                        else ([subtree] if subtree in obs_counts else [])
                    )
                    for subtree in class_tree
                )
            )
            return [result] if (level > 0) and len(result) > 1 else result
            # nest a level to prevent flattening in the parent level unless we're at the root
    else:
        return flattened


def generate_class_tree(
    class_tree: ClassTree[C],
    obs_counts: pd.Series,
    min_frequency: int,
) -> Tuple[Trie[int, SubtreeClassifierSpec], List[C]]:
    """Given a class tree (basic representation as nested lists), and observed counts of each class in
    training data, construct a specification for a tree-structured classifier ensemble in the form of a
    Trie whose data are `SubtreeClassifierSpec` specifying a classifier for each internal node.
    The final constructed tree is a transformation of the input `class_tree` where alterations are made
    only to ensure that the following properties hold:
    1) At least `min_frequency` data points are observed for classes under each node. To satisfy this
      constraint, the leaves of each subtree not meeting this threshold are flattened, or "lifted" to
      become direct descendants of the parent of the low-frequency sub-tree.
    2) Any observed classes in `obs_counts` that are not in the `class_tree` are present as targets for
      some classifier in the final tree. This is accomplished by simply appending the
      observed-but-unspecified classes as targets for the root classifier.
    3) Any unobserved classes that are in the `class_tree` but not in `obs_counts` are removed as
      classifier targets in the final result.
    """
    obs_classes = list(_leaves(class_tree))
    extra_obs = obs_counts[~obs_counts.index.isin(obs_classes)]

    if len(extra_obs) > 0:
        _LOGGER.warning(
            "Observed classes not in class tree: %s; these will be inferred by the root classifier",
            extra_obs.to_dict(),
        )
        class_tree = class_tree + [extra_obs.index.tolist()]

    obs_counts_ = obs_counts[obs_counts > 0].to_dict()
    unobs = set(obs_classes).difference(obs_counts_)
    if len(unobs) > 0:
        _LOGGER.warning("Some classes in the class tree were not observed: %s", unobs)

    class_tree = _lift(class_tree, obs_counts_, min_frequency)
    return _to_tree(class_tree, obs_counts_), list(_leaves(class_tree))


class _NotSet:
    pass


_NOTSET = _NotSet()


T = TypeVar("T")


def _identity(x: T) -> T:
    return x


class SubtreeFitTask(NamedTuple):
    path: tuple[int, ...]
    X: Any2DArray
    y: Any1DArray
    sample_weight: Any1DArray | None


# (base_classifier_template, tasks) → iterable of (path, fitted_classifier)
SubtreeExecutor = Callable[
    [BaseEstimator, Iterable[SubtreeFitTask]],
    Iterable[tuple[tuple[int, ...], ClassifierMixin]],
]


def _generate_fit_tasks(
    tree: Trie[int, SubtreeClassifierSpec],
    X: Any2DArray,
    y: pd.Series,
    sample_weight: Any1DArray | None,
    max_rows_train: int | None,
    random_state: int | None,
) -> Iterator[SubtreeFitTask]:
    Xs: list[Any2DArray] = []
    ys: list[pd.Series] = []
    weights: list[Any1DArray | None] = []
    for subtree_at_path in tree.dfs():
        path = subtree_at_path.path
        subtree = subtree_at_path.tree
        this_level = len(path)
        n_branches = len(subtree.value.class_ranges)
        _LOGGER.info(
            "Preparing fit task for subtree at path %s, depth %d, with %d branches",
            path,
            this_level,
            n_branches,
        )

        # starting back from an earlier branch
        while len(Xs) > this_level:
            Xs.pop()
            ys.pop()
            weights.pop()

        if this_level > 0:
            last_X = Xs[-1]
            last_y = ys[-1]
            last_weight = weights[-1]
            subset = last_y.isin(subtree.value.class_mapping)
            _LOGGER.info(
                "Selecting row subset where y in set of %d classes", len(subtree.value.class_mapping)
            )
            this_X = last_X[subset.values.tolist()]
            this_y = last_y.loc[subset]
            _LOGGER.info(
                "Reduced row count from %d to %d (%2.2f%%)",
                last_X.shape[0],
                this_X.shape[0],
                100 * (last_X.shape[0] - this_X.shape[0]) / last_X.shape[0],
            )
            this_weight: Any1DArray | None = None if last_weight is None else last_weight[subset]
        else:
            this_X = X
            this_y = y
            this_weight = sample_weight

        Xs.append(this_X)
        ys.append(this_y)
        weights.append(this_weight)

        if max_rows_train and this_X.shape[0] > max_rows_train:
            _LOGGER.info(
                "Sampling down to %d rows from %d for training",
                max_rows_train,
                this_X.shape[0],
            )
            sample_ixs = stratified.stratified_sample_ixs(
                this_y,
                max_rows_train,
                random_state=random_state,
            )
            this_X = array_compat.slice_2d(this_X, sample_ixs, None)
            this_y = (this_y.iloc if isinstance(this_y, pd.Series) else this_y)[sample_ixs]
            if this_weight is not None:
                this_weight = (this_weight.iloc if isinstance(this_weight, pd.Series) else this_weight)[
                    sample_ixs
                ]

        yield SubtreeFitTask(
            path=tuple(path),
            X=this_X,
            y=this_y.map(subtree.value.class_mapping),
            sample_weight=this_weight,
        )


def _prepare_fit_tasks(
    tree: Trie[int, SubtreeClassifierSpec],
    X: Any2DArray,
    y: pd.Series,
    sample_weight: Any1DArray | None,
    max_rows_train: int | None,
    random_state: int | None,
) -> parallel.IteratorWithLen[SubtreeFitTask]:
    """Walk the tree DFS, yielding data subsets for each subtree classifier.

    DFS ordering lets each node filter rows from its parent's subset rather than
    scanning the full dataset. Yielding lazily avoids holding all sliced subsets
    in memory simultaneously (~average_depth × dataset_size savings).
    The yielded tasks are fully independent and can be executed in any order.

    Returns an IteratorWithLen so that parallel.yield_all can right-size
    its thread pool without materializing the full task list.
    """
    # tree.dfs() just walks node pointers (no data slicing) — microseconds even
    # for hundreds of nodes.
    n_nodes = sum(1 for _ in tree.dfs())

    return parallel.IteratorWithLen(
        n_nodes, _generate_fit_tasks(tree, X, y, sample_weight, max_rows_train, random_state)
    )


def _sequential_subtree_executor(
    classifier: BaseEstimator, tasks: Iterable[SubtreeFitTask]
) -> Iterable[tuple[tuple[int, ...], ClassifierMixin]]:
    for task in tasks:
        _LOGGER.info(
            "Fitting %s instance on %d rows and %d features",
            type(classifier),
            task.X.shape[0],
            task.X.shape[1],
        )
        yield (
            task.path,
            sklegos_util.fit(
                classifier,
                task.X,
                task.y,
                subset=None,
                sample_weight=task.sample_weight,
                copy=True,
            ),
        )


class TreeStructuredLabelsClassifier(BaseEstimator, TransformerMixin):
    classifier: ClassifierMixin
    class_tree: ClassTree
    min_frequency: int
    max_rows_train: Optional[int]
    prob_dtype: str
    temperature_smoothing: Optional[float]
    pruning: bool
    random_state: Optional[int]
    subtree_executor: SubtreeExecutor | None

    def __init__(
        self,
        class_tree: ClassTree,
        classifier: ClassifierMixin,
        min_frequency: int = 1,
        max_rows_train: Optional[int] = None,
        prob_dtype: str = "float64",
        pruning: bool = False,
        temperature_smoothing: Optional[float] = None,
        random_state: Optional[int] = None,
        subtree_executor: SubtreeExecutor | None = None,
    ):
        """Learn separate classifiers at each internal node of a tree of class labels where there is
        sufficient support to learn a separate classifier. Lower-support internal nodes are aggregated
        upwards until there is sufficient support.

        :param class_tree: a tree of class labels, represented as a list of either class labels or
          sub-trees of class labels, recursively.
        :param classifier: a classifier instance. Must support probabilistic prediction via a `predict_proba`
          method. A clone of this classifier will be fit at each internal node of the ensemble tree.
        :param min_frequency: the minimum number of observations required to fit a classifier at a node.
          Sub-trees with fewer observations will be flattened and aggregated upwards (see `generate_class_tree`).
        :param max_rows_train: if not None, the maximum number of rows to use when fitting each classifier. By the nature of
          the ensemble, most classifiers train on only a small subset of the data, but the root classifier and other
          high-level classifiers may encounter more data than is feasible for training your architecture of choice.
          Using this parameter can allow you to train on much larger datasets for improving lower-level classifiers
          without swamping the higher-level ones with too much data.
        :param prob_dtype: the data type of the probabilities returned by the `predict_proba` method.
          Default is 'float64'. Smaller data types may be used to save memory.
        :param pruning: whether to prune the tree during inference. If True, a classifier will not be
          evaluated on rows where the maximum probability it could achieve is less than a known lower bound from
          previously computed probabilities elsewhere in the tree. In those cases, a smooth probability distribution
          will be set in the pruned rows, indicating total uncertainty. This can significantly speed up inference but
          should only be used in cases where a point estimate is desired. If you want to use the full predicted
          probability distribution (e.g. if you wish to calculate entropy for uncertainty sampling), set this to False.
        :param temperature_smoothing: if not None, apply temperature scaling to the probabilities returned
          by each classifier below the root classifier. The probability that the parent classifier assigns
          to the current subtree is used as a temperature parameter for smoothing the probabilities of the
          of the current classifier for that subtree. When this parameter is > 0, lower confidence of being
          in the current subtree results in more smoothing, with perfect confidence resulting in no smoothing
          at all. Specifically, the exponent for the temperature scaling is
          `temperature_smoothing * p + (1 - temperature_smoothing)`. This parameter is motivated by the
          observation that technically, all classifiers below the root are trained on a subset of the data
          and therefore may be ill-calibrated outside their training domain. The confidence of the parent
          classifier provides an estimate of the applicability of the current classifier, and can be used
          to temper over-confident predictions outside its training domain.
        :param random_state: random seed for reproducibility. Only affects downsampling, which is only performed when
          `max_rows_train` is provided. Defaults to None.
        :param subtree_executor: optional callable that controls how subtree classifiers are fit. When None
          (default), subtrees are fit sequentially in the current process. A custom executor can parallelize
          fitting across threads, processes, or remote nodes. See ``SubtreeExecutor`` for the expected signature.
        """
        if not callable(getattr(classifier, "predict_proba", None)):
            raise ValueError(
                f"Classifier must have a `predict_proba` method; instance of type {type(classifier)} does not"
            )
        if min_frequency <= 0:
            raise ValueError(f"`min_frequency` must be a positive integer; got {min_frequency}")

        clf_params = {f"classifier__{name}": value for name, value in classifier.get_params().items()}
        self.classifier = classifier
        self.class_tree = class_tree
        self.min_frequency = min_frequency
        self.max_rows_train = max_rows_train
        self.random_state = random_state
        self.subtree_executor = subtree_executor
        self.set_inference_params(
            prob_dtype=prob_dtype, pruning=pruning, temperature_smoothing=temperature_smoothing
        )
        self.set_params(
            class_tree=class_tree,
            min_frequency=min_frequency,
            max_rows_train=max_rows_train,
            **clf_params,
        )

    def set_inference_params(
        self,
        pruning: bool | _NotSet = _NOTSET,
        prob_dtype: str | _NotSet = _NOTSET,
        temperature_smoothing: float | None | _NotSet = _NOTSET,
    ):
        """Set inference parameters for the classifier. This is useful for setting parameters at inference time that
        only affect inference and not training, such as the use of pruning or lower-precision dtypes as optimizations.
        See `__init__` for details.
        """
        new_params: dict[str, Any] = dict()
        if not isinstance(temperature_smoothing, _NotSet):
            if temperature_smoothing is not None:
                validation.check_scalar(
                    temperature_smoothing,
                    "temperature_smoothing",
                    float,
                    min_val=0.0,
                    max_val=1.0,
                    include_boundaries="right",
                )
            _LOGGER.info("Setting temperature_smoothing to %s", temperature_smoothing)
            self.temperature_smoothing = temperature_smoothing
            new_params["temperature_smoothing"] = temperature_smoothing
        if not isinstance(pruning, _NotSet):
            _LOGGER.info("Setting pruning to %s", pruning)
            self.pruning = pruning
            new_params["pruning"] = pruning
        if not isinstance(prob_dtype, _NotSet):
            if np.dtype(prob_dtype).kind != "f":
                raise ValueError(f"prob_dtype must be a floating point dtype; got {prob_dtype}")
            _LOGGER.info("Setting prob_dtype to %s", prob_dtype)
            self.prob_dtype = prob_dtype
            new_params["prob_dtype"] = prob_dtype

        if new_params:
            self.set_params(**new_params)

    def fit(
        self,
        X: Any2DArray,
        y: Any1DArray,
        sample_weight: Optional[Any1DArray] = None,
    ) -> "TreeStructuredLabelsClassifier":
        # TODO: lower-bound sklearn to 1.6 and use
        #  `X, y = validate_data(self, X, y, skip_check_array=True, dtype=None, copy=False)`
        #  to set these metadata attributes (`validate_data` was added in that version).
        #  The root classifier will handle any other data validation that it requires.
        if isinstance(X, pd.DataFrame):
            self.feature_names_in_ = X.columns.values
        self.n_features_in_ = X.shape[1]

        y = pd.Series(y, copy=False)  # to enable .isin() and .map() calls below
        label_counts_obs = y.value_counts()
        self.tree_, self.classes_ = generate_class_tree(
            self.class_tree, label_counts_obs, self.min_frequency
        )

        tasks = _prepare_fit_tasks(
            self.tree_, X, y, sample_weight, self.max_rows_train, self.random_state
        )

        executor = self.subtree_executor or _sequential_subtree_executor
        _LOGGER.info(
            "Dispatching fit tasks via %s",
            executor.__name__ if hasattr(executor, "__name__") else type(executor),
        )
        for path, fitted_clf in executor(self.classifier, tasks):
            self.tree_[path].value.classifier = fitted_clf

        return self

    def _predict_proba(self, X: Any2DArray, log: bool) -> pd.DataFrame:  # noqa: C901
        n_rows = X.shape[0]
        n_classes = len(self.classes_)
        shape = (n_rows, n_classes)
        if log:
            mul_op: np.ufunc = np.add
            pow_op: np.ufunc = np.multiply
            to_prob_op: Callable[[np.ndarray], np.ndarray] = np.exp
            probs = np.zeros(shape, dtype=self.prob_dtype)
            max_prob_lower_bound = np.full((n_rows,), -np.log(n_classes))
            supports_log_proba = hasattr(self.classifier, "predict_log_proba")

            def p_1_over_n(n: int):
                return -np.log(n)

            if not supports_log_proba:
                _LOGGER.warning(
                    "Classifier of type %s does not support `predict_log_proba`; implementation for %s will "
                    "not be optimal",
                    type(self.classifier),
                    type(self),
                )

                def to_probs(clf, X):
                    return np.log(clf.predict_proba(X))

            else:

                def to_probs(clf, X):
                    return clf.predict_log_proba(X)

            def normalize_in_place(probs: np.ndarray):
                # normalize to probability distribution in log space
                np.subtract(probs, np.logaddexp.reduce(probs, axis=1, keepdims=True), out=probs)

        else:
            mul_op = np.multiply
            pow_op = np.power
            to_prob_op = _identity
            probs = np.ones(shape, dtype=self.prob_dtype)
            max_prob_lower_bound = np.full((n_rows,), 1.0 / n_classes)

            def p_1_over_n(n: int):
                return 1.0 / n

            def to_probs(clf, X):
                return clf.predict_proba(X)

            def normalize_in_place(probs: np.ndarray):
                normalize(probs, norm="l1", axis=1, copy=False)

        validation.check_is_fitted(self)
        pruning = self.pruning
        smoothing = self.temperature_smoothing
        pruned_subtrees: dict[Sequence[int], np.ndarray] = {}
        for subtree_at_path in self.tree_.dfs():
            path = subtree_at_path.path
            clf_spec: SubtreeClassifierSpec = subtree_at_path.tree.value
            start_ix, end_ix = clf_spec.range
            n_leaves = end_ix - start_ix
            n_branches = len(clf_spec.class_ranges)
            parent_prob = probs[:, start_ix : start_ix + 1].copy()
            # we keep this 2D for easier broadcasting where it's needed, and we copy it because we'll be mutating its
            # parent `probs` array in place
            inference_ixs: slice | np.ndarray = slice(None)
            n_inference_rows = n_rows
            if pruning and path:
                prev_pruned_ixs: np.ndarray | None = next(
                    (
                        p
                        for i in range(len(path) - 1, -1, -1)
                        if (p := pruned_subtrees.get(path[:i])) is not None
                    ),
                    None,
                )
                inference_ixs = parent_prob[:, 0] > max_prob_lower_bound
                n_inference_rows = inference_ixs.sum()
                all_pruned_ixs = ~inference_ixs
                if prev_pruned_ixs is not None:
                    # subtree above this was pruned; set *only* the newly pruned probabilities to P(parent) / n_leaves
                    newly_pruned_ixs = all_pruned_ixs & ~prev_pruned_ixs
                else:
                    newly_pruned_ixs = all_pruned_ixs
                if newly_pruned_ixs.any():
                    _LOGGER.debug(
                        "Multiplying probs by %f for %d rows and %d classes",
                        1 / n_leaves,
                        newly_pruned_ixs.sum(),
                        n_leaves,
                    )
                    probs[newly_pruned_ixs, start_ix:end_ix] = mul_op(
                        probs[newly_pruned_ixs, start_ix : start_ix + 1], p_1_over_n(n_leaves)
                    )
                    pruned_subtrees[path] = all_pruned_ixs
            if pruning and n_inference_rows < n_rows:
                _LOGGER.debug(
                    "Pruning to %d out of %d rows (%2.2f%%) for inference at path %s",
                    n_inference_rows,
                    n_rows,
                    100 * n_inference_rows / n_rows,
                    path,
                )
                if n_inference_rows > 0:
                    X_inf = array_compat.slice_2d(X, inference_ixs, None)
                    sub_probs = to_probs(clf_spec.classifier, X_inf).astype(self.prob_dtype, copy=False)
                else:
                    # don't attempt inference on empty dataframe since some classifiers will fail
                    sub_probs = np.empty((0, n_branches), dtype=self.prob_dtype)
            else:
                # no pruning; use all rows
                sub_probs = to_probs(clf_spec.classifier, X).astype(self.prob_dtype, copy=False)
                inference_ixs = slice(None)

            if smoothing:
                temperature = to_prob_op(parent_prob[inference_ixs])
                # when smoothing == 1, the temperature exponent is just the parent probability; lower probability,
                # less certainty, more smoothing
                if smoothing < 1.0:
                    # interpolate temperature exponent between P(parent) and 1:
                    # temperature = smoothing * P(parent) + (1 - smoothing) * 1
                    # temperature will never be lower than P(parent) (no more smoothing than using P(parent))
                    # or higher than 1 (no *increase* of concentration beyond that of the base probabilities).
                    temperature = np.multiply(temperature, smoothing, out=temperature)
                    np.add(temperature, 1.0 - smoothing, out=temperature)
                pow_op(sub_probs, temperature, out=sub_probs)
                normalize_in_place(sub_probs)

            for i, (start, end) in enumerate(clf_spec.class_ranges):
                # map to correct column indices and accumulate by chaining conditional probabilities
                probs[inference_ixs, start:end] = mul_op(
                    probs[inference_ixs, start : start + 1], sub_probs[:, [i]]
                )
                if pruning:
                    branch_width = end - start
                    max_prob_lower_bound_in_subtree = (
                        probs[:, start]
                        if branch_width == 1
                        else mul_op(probs[:, start], p_1_over_n(branch_width))
                    )
                    max_prob_lower_bound = np.maximum(
                        max_prob_lower_bound, max_prob_lower_bound_in_subtree
                    )

        index = X.index if isinstance(X, pd.DataFrame) else None
        return pd.DataFrame(probs, index=index, columns=self.classes_)

    def predict_proba(self, X: Any2DArray) -> pd.DataFrame:
        return self._predict_proba(X, log=False)

    def predict_log_proba(self, X: Any2DArray) -> pd.DataFrame:
        return self._predict_proba(X, log=True)

    def predict(self, X: Any2DArray) -> pd.Series:
        probs = self.predict_proba(X)
        return probs.idxmax(axis=1)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        state["subtree_executor"] = None
        return state

    def __setstate__(self, state: dict[str, Any]):
        missing_param_defaults = {
            name: default
            for name, default in (
                ("pruning", False),
                ("max_rows_train", None),
                ("random_state", None),
                ("subtree_executor", None),
            )
            if name not in state
        }
        state.update(missing_param_defaults)
        # ^ for compatibility with pickles created before these parameters were added
        super().__setstate__(state)

        if missing_param_defaults:
            _LOGGER.warning(
                "Loading a pickled TreeStructuredLabelsClassifier without some parameters set; "
                "The following defaults will be set: %s. See docstring for interpretation of these parameters. "
                "The defaults will not alter the behavior of this model either at training or inference time.",
                missing_param_defaults,
            )
            self.set_params(**missing_param_defaults)
