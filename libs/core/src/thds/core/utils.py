import os
from collections.abc import MutableMapping
from typing import Any, Dict, List, Tuple


def flatten(d, parent_key="", sep=".") -> Dict[str, Any]:
    """
    flattens a dictionary using a seperator.

    Example
    ---------

    d = {"a": {"b": {"c": 1}}}
    fd = flatten(d, sep=".")
    print(fd)
    > {"a.b.c": 1}
    """
    items: List[Tuple[str, Any]] = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())  # type: ignore
        else:
            items.append((new_key, v))
    return dict(items)


class Map(dict):
    def __init__(self, *args, **kwargs):
        super(Map, self).__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    if isinstance(v, dict):
                        self[k] = Map(v)
                    else:
                        self[k] = v

        if kwargs:
            for k, v in kwargs.items():
                if isinstance(v, dict):
                    self[k] = Map(v)
                else:
                    self[k] = v

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(Map, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(Map, self).__delitem__(key)
        del self.__dict__[key]

    def set_value(self, dot_path: str, val):
        ref = self
        path = dot_path.split(".")
        for k in path[:-1]:
            ref = ref.__getattr__(k)
        ref.__setattr__(path[-1], val)


class ProgressList:
    """Helper class that provides a terminal progress list."""

    def __init__(self, labels):
        self._labels = labels
        self._labels_map = {label: i for i, label in enumerate(self._labels)}
        self._num_items = len(self._labels)
        if os.get_terminal_size().lines >= len(self._labels):
            for label in self._labels:
                print(f"🔳 {label}")

    def __len__(self):
        return len(self._labels)

    def index(self, label):
        return self._labels_map.get(label)

    def update_label(self, old_label: str, new_label: str):
        i = self.index(old_label)
        if os.get_terminal_size().lines >= len(self._labels):
            num_items = self._num_items
            update_line = f"\033[{num_items - i}A"
            back_to_bottom = f"\033[{num_items - i - 1}B"
            if i == num_items - 1:
                print(f"{update_line}🔳 {new_label}\033[K")
            else:
                print(f"{update_line}🔳 {new_label}\033[K{back_to_bottom}")
        else:
            print(f"🔳 {new_label}\033[K")
        self._labels[i] = new_label
        del self._labels_map[old_label]
        self._labels_map[new_label] = i

    def mark_completed(self, label, icon="✅"):
        if os.get_terminal_size().lines >= len(self._labels):
            i = self.index(label)
            num_items = self._num_items
            update_line = f"\033[{num_items - i}A"
            back_to_bottom = f"\033[{num_items - i - 1}B"
            if i == num_items - 1:
                print(f"{update_line}{icon}  {label}\033[K")
            else:
                print(f"{update_line}{icon}  {label}\033[K{back_to_bottom}")
        else:
            print(f"{icon}  {label}")
