"""In theory, our core concept supports multiple different Runner 'types' being registered and used at the time of remote entry.

In practice we only have a single Runner type registered, the MemoizingPicklingRunner.
"""

import typing as ty

from thds.core import stack_context

RUNNER_ENTRY_COUNT = stack_context.StackContext("runner_entry_count", 0)


def entry_count() -> int:
    return RUNNER_ENTRY_COUNT()


class EntryHandler(ty.Protocol):
    def __call__(self, *__args: str) -> ty.Any:
        ...  # pragma: nocover


ENTRY_HANDLERS: ty.Dict[str, EntryHandler] = dict()


def register_entry_handler(name: str, mh: EntryHandler) -> None:
    ENTRY_HANDLERS[name] = mh


def run_named_entry_handler(name: str, *args: str) -> None:
    with RUNNER_ENTRY_COUNT.set(RUNNER_ENTRY_COUNT() + 1):
        ENTRY_HANDLERS[name](*args)
