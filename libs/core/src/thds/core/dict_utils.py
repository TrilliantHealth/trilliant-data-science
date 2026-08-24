import re
import typing as ty
import warnings
from collections import defaultdict

VT = ty.TypeVar("VT")
V = ty.TypeVar("V")
K = ty.TypeVar("K")


def _get_valid_variable_name(var: str):
    """
    given a string returns the string formatted as a proper python variable name.
    Credit: https://stackoverflow.com/questions/3303312/how-do-i-convert-a-string-to-a-valid-variable-name-in-python
    """
    return re.sub(r"\W+|^(?=\d)", "_", var)


def _flatten_gen(
    d: ty.Mapping[K, V], parent_key: tuple[K, ...] = ()
) -> ty.Iterable[tuple[tuple[K, ...], V]]:
    for k, v in d.items():
        new_key = parent_key + (k,) if parent_key else (k,)
        if isinstance(v, ty.Mapping):
            yield from _flatten_gen(v, new_key)
        else:
            yield new_key, v


def unflatten(flat_d: dict[tuple[K, ...], V]):
    """Given a flattened dictionary returns the un-flatten representation."""
    unflatten_dict: dict[K, ty.Any] = {}
    for path, val in flat_d.items():
        dict_ref = unflatten_dict
        for p in path[:-1]:
            dict_ref[p] = dict_ref.get(p) or {}
            dict_ref = dict_ref[p]
        dict_ref[path[-1]] = val
    return unflatten_dict


def flatten(d: ty.Mapping[K, ty.Any], parent_key: tuple[K, ...] = ()) -> dict[tuple[K, ...], ty.Any]:
    """
    flattens a mapping (usually a dict), returning a dictionary of the flattened keys and values.

    Each key is the tuple of path segments leading to its value. See `flatten_with_str_keys` if you
    want those segments joined into a single string.

    ## Example

    ```python
    d = {"a": {"b": {"c": 1}}}
    fd = flatten(d)
    print(dict(fd))
    > {('a', 'b', 'c'): 1}
    ```
    """
    return dict(_flatten_gen(d, parent_key))


def flatten_with_str_keys(
    d: ty.Mapping[str, ty.Any], parent_key: ty.Optional[str] = None, sep: str = "."
) -> dict[str, ty.Any]:
    """
    flattens a mapping (usually a dict), returning a dictionary of the flattened keys and values.

    ## Example

    ```python
    d = {"a": {"b": {"c": 1}}}
    fd = flatten_with_str_keys(d)
    print(dict(fd))
    > {"a.b.c": 1}
    ```
    """
    parent_key_tuple = tuple(parent_key.split(sep)) if parent_key else ()
    return {sep.join(k): v for k, v in dict(_flatten_gen(d, parent_key_tuple)).items()}


class DotDict(dict[str, VT], ty.MutableMapping[str, VT]):
    """A python dictionary that acts like an object."""

    _new_to_orig_keys: dict[str, str] = dict()
    _hidden_data: dict[str, ty.Any] = dict()

    def _get_hidden_data(self, identifier: str) -> ty.Any:
        return self._hidden_data.get(identifier)

    def _construct(self, mapping: ty.Mapping) -> None:
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

    def __init__(
        self,
        *args: ty.Mapping[str, VT],
        convert_keys_to_identifiers: bool = False,
        **kwargs: VT,
    ):
        self._hidden_data["convert_keys_to_identifiers"] = convert_keys_to_identifiers
        if convert_keys_to_identifiers:
            warnings.warn("automatically converting keys into identifiers. Data loss might occur.")
        for arg in args:
            if isinstance(arg, dict):
                self._construct(mapping=arg)
            else:
                raise ValueError(arg)
        if kwargs:
            self._construct(mapping=kwargs)

    def __getattr__(self, key: str) -> VT:
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key: str, value: VT) -> None:
        if key.startswith("__") and key.endswith("__"):
            object.__setattr__(self, key, value)
        else:
            self.__setitem__(key, value)

    def __setitem__(self, key: str, value: VT):
        super(DotDict, self).__setitem__(key, value)

    def __delattr__(self, key: str) -> None:
        self.__delitem__(key)

    def __delitem__(self, key: str) -> None:
        super(DotDict, self).__delitem__(key)
        del self.__dict__[key]

    def to_dict(self, orig_keys: bool = False) -> dict[str, VT]:
        convert_keys_to_identifiers = self._get_hidden_data("convert_keys_to_identifiers")
        d: dict[str, VT] = dict()
        for k, v in self.items():
            if isinstance(v, DotDict):
                d[(self._new_to_orig_keys[k] if orig_keys and convert_keys_to_identifiers else k)] = (
                    v.to_dict(orig_keys)  # type: ignore[assignment]
                )
            else:
                d[(self._new_to_orig_keys[k] if orig_keys and convert_keys_to_identifiers else k)] = v
        return d

    def get_value(self, dot_path: str) -> ty.Optional[VT]:
        """Get a value given a dotted path to the value.

        Example
        -------

        dd = DotDict(a={"b": 100})
        assert dd.get_value("a.b") == 100
        """
        path = dot_path.split(".")
        ref: DotDict[ty.Any] = self
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
        """Set a value given a dotted path."""
        ref: DotDict = self
        path = dot_path.split(".")
        for k in path[:-1]:
            try:
                ref = getattr(ref, k)
            except AttributeError:
                ref[k] = DotDict()
                ref = ref[k]
        ref.__setattr__(path[-1], val)

    def flatten(self) -> dict[str, VT]:
        return flatten_with_str_keys(self)


def merge_dicts(*dicts: dict[ty.Any, ty.Any], default: ty.Any = None) -> dict[ty.Any, ty.Any]:
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
    merged_dict: dict[str, list[ty.Any]] = defaultdict(lambda: [default for _ in range(len(dicts))])
    for i, d in enumerate(dicts):
        for k, v in d.items() if isinstance(d, dict) else {}:
            if isinstance(v, dict):
                merged_dict[k] = merge_dicts(*[a.get(k, {}) for a in dicts])  # type: ignore
            else:
                merged_dict[k][i] = v
    return dict(merged_dict)
