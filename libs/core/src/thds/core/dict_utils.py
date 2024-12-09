import re
import warnings
from collections import defaultdict
from typing import Any, Dict, Generator, List, Mapping, MutableMapping, Optional, Tuple, TypeVar

DEFAULT_SEP = "."
VT = TypeVar("VT")


def _get_valid_variable_name(var: str):
    """
    given a string returns the string formatted as a proper python variable name.
    Credit: https://stackoverflow.com/questions/3303312/how-do-i-convert-a-string-to-a-valid-variable-name-in-python
    """
    return re.sub(r"\W+|^(?=\d)", "_", var)


def _flatten_gen(
    d: Mapping, parent_key: str = "", sep: str = DEFAULT_SEP
) -> Generator[Tuple[str, Any], None, None]:
    """
    flattens a mapping (usually a dict) using a separator, returning a generator of the flattened keys and values.

    Example
    ---------

    d = {"a": {"b": {"c": 1}}}
    fd = flatten(d, sep=".")
    print(dict(fd))
    > {"a.b.c": 1}
    """
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, Mapping):
            yield from _flatten_gen(v, new_key, sep=sep)
        else:
            yield new_key, v


def unflatten(flat_d: Dict[str, Any], sep: str = DEFAULT_SEP):
    """Given a flattened dictionary returns the un-flatten representation."""
    unflatten_dict: Dict[str, Any] = {}
    for path, val in flat_d.items():
        dict_ref = unflatten_dict
        path_parts = path.split(sep)
        for p in path_parts[:-1]:
            dict_ref[p] = dict_ref.get(p) or {}
            dict_ref = dict_ref[p]
        dict_ref[path_parts[-1]] = val
    return unflatten_dict


def flatten(d: Mapping, parent_key: str = "", sep: str = DEFAULT_SEP) -> Dict[str, Any]:
    return dict(_flatten_gen(d, parent_key, sep))


class DotDict(MutableMapping[str, VT]):
    """A python dictionary that acts like an object."""

    _new_to_orig_keys: Dict[str, str] = dict()
    _hidden_data: Dict[str, Any] = dict()

    def _get_hidden_data(self, identifier: str) -> Any:
        return self._hidden_data.get(identifier)

    def _construct(self, mapping: Mapping) -> None:
        convert_keys_to_identifiers = self._get_hidden_data("convert_keys_to_identifiers")
        for k, v in mapping.items():
            new_key = _get_valid_variable_name(k) if convert_keys_to_identifiers else k
            if convert_keys_to_identifiers:
                self._new_to_orig_keys[new_key] = k
            if isinstance(v, dict):
                self[new_key] = DotDict(v)  # type: ignore
            elif isinstance(v, (list, tuple, set)):
                self[new_key] = v.__class__([DotDict(iv) if isinstance(iv, dict) else iv for iv in v])  # type: ignore
            else:
                self[new_key] = v

    def __init__(self, *args, convert_keys_to_identifiers: bool = False, **kwargs):
        self._hidden_data["convert_keys_to_identifiers"] = convert_keys_to_identifiers
        if convert_keys_to_identifiers:
            warnings.warn("automatically converting keys into identifiers. Data loss might occur.")
        for arg in args:
            if isinstance(arg, dict):
                self._construct(mapping=arg)
        if kwargs:
            self._construct(mapping=kwargs)

    def __getattr__(self, key: str) -> VT:
        return self[key]

    def __setattr__(self, key: str, value: VT) -> None:
        self.__setitem__(key, value)

    def __setitem__(self, key: str, value: VT):
        self.__dict__.update({key: value})

    def __delattr__(self, key: str) -> None:
        self.__delitem__(key)

    def __delitem__(self, key: str) -> None:
        super(DotDict, self).__delitem__(key)
        del self.__dict__[key]

    def __getitem__(self, key: str) -> VT:
        return self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self) -> int:
        return len(self.__dict__)

    def to_dict(self, orig_keys: bool = False) -> Dict[str, VT]:
        convert_keys_to_identifiers = self._get_hidden_data("convert_keys_to_identifiers")
        d: Dict[str, VT] = dict()
        for k, v in self.items():
            if isinstance(v, DotDict):
                d[
                    self._new_to_orig_keys[k] if orig_keys and convert_keys_to_identifiers else k
                ] = v.to_dict(
                    orig_keys
                )  # type: ignore[assignment]
            else:
                d[self._new_to_orig_keys[k] if orig_keys and convert_keys_to_identifiers else k] = v
        return d

    def get_value(self, dot_path: str) -> Optional[VT]:
        """Get a value given a dotted path to the value.

        Example
        -------

        dd = DotDict(a={"b": 100})
        assert dd.get_value("a.b") == 100
        """
        path = dot_path.split(".")
        ref: DotDict[Any] = self
        for k in path[:-1]:
            if isinstance(ref, DotDict) and k in ref:
                ref = ref[k]
            else:
                return None
        try:
            return ref[path[-1]]
        except KeyError:
            return None

    def set_value(self, dot_path: str, val: VT) -> None:
        """Set a vlaue given a dotted path."""
        ref = self
        path = dot_path.split(".")
        try:
            for k in path[:-1]:
                ref = getattr(ref, k)
        except AttributeError:
            raise KeyError(f"can't set path {dot_path} with parts {path}.")
        ref.__setattr__(path[-1], val)


def merge_dicts(*dicts: Dict[Any, Any], default: Any = None) -> Dict[Any, Any]:
    """Merges similar dictionaries into one dictionary where the resulting values are a list of values from the
    original dicts. If a dictionary does not have a key the default value will be used (defaults to None).

    Example
    --------

    assert merge_dicts(
        {"a": 100, "b": {"c": 200, "d": 300}, "e": [1, 2]},
        {"a": 200, "b": {"c": 300}, "e": [3, 4], "f": 300}
    ) == {
        "a": [100, 200],
        "b": {
            "c": [200, 300],
            "d": [300, None]
        },
        "e": [[1,2], [3,4]],
        "f": [None, 300]
    }
    """
    merged_dict: Dict[str, List[Any]] = defaultdict(lambda: [default for _ in range(len(dicts))])
    for i, d in enumerate(dicts):
        for k, v in d.items() if isinstance(d, dict) else {}:
            if isinstance(v, dict):
                merged_dict[k] = merge_dicts(*[a.get(k, {}) for a in dicts])  # type: ignore
            else:
                merged_dict[k][i] = v
    return dict(merged_dict)
