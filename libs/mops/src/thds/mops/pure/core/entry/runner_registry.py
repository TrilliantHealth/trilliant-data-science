"""In theory, our core concept supports multiple different Runner 'types' being registered and used at the time of remote entry.

In practice we only have a single Runner type registered, the MemoizingPicklingRunner.
"""

import typing as ty


class EntryHandler(ty.Protocol):
    def __call__(self, *__args: str) -> ty.Any: ...  # pragma: nocover


ENTRY_HANDLERS: ty.Dict[str, EntryHandler] = dict()


def register_entry_handler(name: str, mh: EntryHandler) -> None:
    ENTRY_HANDLERS[name] = mh


def run_named_entry_handler(name: str, *args: str) -> None:
    ENTRY_HANDLERS[name](*args)
