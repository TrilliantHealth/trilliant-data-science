import collections
import dataclasses
import functools
import typing as ty

import typing_extensions as te

K = ty.TypeVar("K", bound=ty.Hashable)
T = ty.TypeVar("T")
U = ty.TypeVar("U")


@dataclasses.dataclass(frozen=False)
class IndexedTree(ty.Generic[K, T]):
    value: T
    children: ty.MutableMapping[K, te.Self]
    parent: ty.Optional[te.Self] = None

    def __post_init__(self):
        self.children = {
            k: dataclasses.replace(child, parent=self) for k, child in self.children.items()
        }

    def dfs(self, _prefix: ty.Tuple[K, ...] = ()) -> ty.Iterator["TreeAtPath[K, T]"]:
        yield TreeAtPath(_prefix, self)
        for k, child in self.children.items():
            yield from child.dfs((*_prefix, k))

    def bfs(self):
        queue = collections.deque([TreeAtPath((), self)])
        while queue:
            next_ = queue.popleft()
            yield next_
            for k, child in next_.tree.children.items():
                queue.append(TreeAtPath((*next_.path, k), child))

    @property
    def leaves(self) -> ty.Iterator["TreeAtPath[K, T]"]:
        return filter(lambda node: not node.tree.children, self.dfs())

    @property
    def parents(self) -> ty.Iterator["IndexedTree[K, T]"]:
        node = self
        while (p := node.parent) is not None:
            yield p
            node = p

    @property
    def depth(self) -> int:
        return 1 + max((child.depth for child in self.children.values()), default=0)

    def __getitem__(self, item: ty.Iterable[K]) -> te.Self:
        return functools.reduce(lambda node, key: node.children[key], item, self)

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, type(self))
            and (self.value == other.value)
            and (self.children == other.children)
        )
        # skip parent comparison


Trie = IndexedTree  # alias


@dataclasses.dataclass(frozen=True)
class TreeAtPath(ty.Generic[K, T]):
    path: ty.Sequence[K]
    tree: IndexedTree[K, T]

    @property
    def depth(self) -> int:
        return len(self.path)


def trie_from_values(values: ty.Iterable[ty.Tuple[ty.Sequence[K], T]], default: T) -> Trie[K, T]:
    root: Trie[K, T] = Trie(default, {})
    for path, value in values:
        node = root
        for key in path:
            node = node.children.setdefault(key, Trie(default, {}, node))
        node.value = value
    return root


def trie_from_counts(
    value_counts: ty.Iterable[ty.Tuple[ty.Sequence[K], int]], count_prefixes: bool = False
) -> Trie[K, int]:
    if count_prefixes:
        root: Trie[K, int] = Trie(0, {})
        for path, count in value_counts:
            node = root
            node.value += count
            for value in path:
                node = node.children.setdefault(value, Trie(0, {}, node))
                node.value += count
        return root
    else:
        return trie_from_values(value_counts, 0)
        # only count at the terminal node of the path; the rest are 0


def tree_acc(
    f: ty.Callable[[T, ty.Mapping[K, IndexedTree[K, U]], ty.Tuple[K, ...]], U],
    tree: IndexedTree[K, T],
    path: ty.Tuple[K, ...] = (),
) -> IndexedTree[K, U]:
    children = {k: tree_acc(f, child, (*path, k)) for k, child in tree.children.items()}
    return IndexedTree(value=f(tree.value, children, path), children=children)


def tree_map(f: ty.Callable[[T, ty.Tuple[K, ...]], U], tree: IndexedTree[K, T]) -> IndexedTree[K, U]:
    return tree_acc(lambda value, _, path: f(value, path), tree)
