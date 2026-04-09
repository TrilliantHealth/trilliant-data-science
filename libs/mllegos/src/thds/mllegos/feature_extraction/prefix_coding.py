import functools
import heapq
from copy import deepcopy
from typing import Collection, Mapping, Optional, Tuple, TypeVar, Union

import pandas as pd

from ..util import aliases
from ..util.heap import HeapItem
from ..util.tree import IndexedTree, TreeAtPath, Trie, trie_from_counts

T = TypeVar("T")


def longest_known_prefix(prefixes: Collection[str], min_prefix_len: int, code: str) -> Optional[str]:
    if code in prefixes:
        return code
    ps = (code[:i] for i in range(len(code) - 1, min_prefix_len - 1, -1))
    known_prefixes = filter(prefixes.__contains__, ps)
    return next(known_prefixes, None)


def prefix_normalizer(prefixes: Collection[str], min_prefix_len: int = 1) -> aliases.Normalizer[str]:
    return functools.partial(longest_known_prefix, prefixes, min_prefix_len)


def _heapitem(t: TreeAtPath[T, int]) -> HeapItem[TreeAtPath[T, int], Tuple[int, int]]:
    key = (-len(t.path), t.tree.value)
    return HeapItem((t, key))


def _total_prefix_count(t: IndexedTree[T, int]) -> int:
    return sum((t_.value for t_ in t.parents), start=t.value)


def optimize_prefix_coding(
    code_counts: Union[Mapping[str, int], pd.Series],
    min_count: int,
) -> Mapping[str, int]:
    """For a set of observed counts of code strings coming from a code system which encodes hierarchical
    information using prefixes, find a maximal set of code prefixes such that:
    1) every prefix occurs in `code_counts` at least `min_count` times
    2) no prefix is shorter than `min_prefix_len`

    :param code_counts: A Series mapping code strings (the index) to their observed counts (the values)
    :param min_count: The minimum support allowed for any code or prefix after pruning
    :param min_prefix_len: The minimum length of a prefix to be retained in the result
    :return: the final counts of all remaining prefixes after pruning
    """
    trie: Trie[str, int] = trie_from_counts((str(code), count) for code, count in code_counts.items())
    aggregated = aggregate_count_trie(trie, min_count, copy=False)
    return {"".join(t.path): t.tree.value for t in aggregated.dfs() if t.tree.value >= min_count}


def aggregate_count_trie(
    trie: Trie[T, int],
    min_count: int,
    copy: bool = True,
) -> Trie[T, int]:
    leaves = list(map(_heapitem, filter(lambda t: t.tree.value < min_count, trie.dfs())))
    heapq.heapify(leaves)
    if copy:
        trie = deepcopy(trie)

    while leaves:
        longest_rarest: TreeAtPath[T, int]
        longest_rarest, (_len, count) = heapq.heappop(leaves)
        node = longest_rarest.tree
        if node.value != count:
            # node has been updated since it was added to the heap and the heap entry is stale
            if node.value < min_count:
                heapq.heappush(leaves, _heapitem(longest_rarest))
                # re-enqueue the node with its updated value
            continue

        if count >= min_count:
            # no more aggregation needed
            continue

        parent = longest_rarest.tree.parent
        if parent is not None:
            # node is rare; aggregate counts to parent
            parent.value += count

    return trie
