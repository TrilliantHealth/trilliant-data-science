from collections import ChainMap, deque
from types import MemberDescriptorType, ModuleType
from typing import Any, Collection, Dict, Generic, Iterator, Optional, Set, Tuple, Type, Union

from .type_utils import T, typename

CLASS_DUNDER_NAMES: Set[str] = (
    set(vars(object))
    .union(dir(ModuleType("")))
    .union(
        {
            "__name__",
            "__file__",
            "__module__",
            "__dict__",
            "__weakref__",
            "__builtins__",
        }
    )
)

#######################################################
# Store for naming types from many modules succinctly #
# and keeping track of computations on them           #
#######################################################


class TypeCache(Generic[T]):
    """Key-value store for type objects whose main purpose is to provide canonical names for types,
    given a set of modules they are defined in. Passing modules via keyword allows you to override the
    name of a module in the final dotted name of a type, in case there is some display concern where that
    would be useful, or when generating an external-facing interface where stability is required even
    while the internal module structure may be changing. Also allows storage of metadata about types;
    this is what is parameterized by the `T` type variable.

    Example:

        import builtins, datetime
        from thds.attrs_utils import type_cache

        types = TypeCache(internal=[type_cache], python=[builtins, datetime])

        print(types.name_of(int))
        print(types.name_of(datetime.date))
        print(types.name_of(type_cache.TypeCache))

        # python.int
        # python.date
        # internal.TypeCache
    """

    def __init__(self, **modules: Union[ModuleType, Collection[ModuleType]]):
        name_lookups = []
        for module_name, m in modules.items():
            ms: Collection[ModuleType]
            ms = [m] if isinstance(m, ModuleType) else m
            for module in ms:  # type: ignore
                names = {
                    id(obj): name
                    for name, obj in object_names_recursive(module, cache=set(), name=module_name)
                }
                name_lookups.append(names)

        self.type_names = ChainMap(*name_lookups)
        self.schemas: Dict[int, T] = {}

    def name_of(self, type_: Type):
        if id(type_) in self.type_names:
            return self.type_names[id(type_)]
        return typename(type_)

    def base_name_of(self, type_: Type):
        return self.name_of(type_).split(".")[-1]

    def __setitem__(self, key: Type, value: T):
        # some type objects which are distinct (different `id`) actually compare equal, so we have to
        # store and look up distinct types by `id` to avoid collisions and therefore ambiguous naming;
        # otherwise, the `name_of` a type could change during the course of program execution as the
        # contents of the cache are populated
        self.schemas[id(key)] = value

    def __getitem__(self, key: Type) -> T:
        return self.schemas[id(key)]

    def __contains__(self, key: Type) -> bool:
        return id(key) in self.schemas

    def pop(self, key: Type) -> T:
        return self.schemas.pop(id(key))


def object_names_recursive(
    module: Union[ModuleType, Type],
    cache: Set[int],
    name: Optional[str] = None,
) -> Iterator[Tuple[str, Any]]:
    """Names of objects, recursing into namespaces of classes, breadth-first"""
    q = deque([(name or module.__name__, module)])
    while q:
        prefix, module = q.popleft()
        for name, obj in vars(module).items():
            if (
                name not in CLASS_DUNDER_NAMES
                and not isinstance(obj, MemberDescriptorType)
                and id(obj) not in cache
            ):
                yield f"{prefix}.{name}", obj
                cache.add(id(obj))
                if isinstance(obj, type):  # recurse
                    q.append((f"{prefix}.{name}", obj))
