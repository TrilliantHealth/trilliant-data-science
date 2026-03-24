"""In theory, our core concept supports multiple different Runner 'types' being registered and used at the time of remote entry.

In practice we only have a single Runner type registered, the MemoizingPicklingRunner.
"""

import typing as ty

# Exit code used when the remote function raised an exception. The exception itself is
# serialized to blob storage for the caller to retrieve; this non-zero exit signals to
# the surrounding infrastructure (k8s, Databricks, etc.) that the run was not successful,
# so that it does not silently treat the job as having succeeded.
#
# Runtimes that invoke this entrypoint as a subprocess (subprocess_shim, dbxtend) should
# catch CalledProcessError with this specific code and allow normal result retrieval to
# proceed, since the exception is already in blob storage.
MOPS_EXCEPTION_EXIT_CODE = 46


class EntryHandler(ty.Protocol):
    def __call__(self, *__args: str) -> None | Exception: ...  # pragma: nocover


ENTRY_HANDLERS: ty.Dict[str, EntryHandler] = dict()


def register_entry_handler(name: str, mh: EntryHandler) -> None:
    ENTRY_HANDLERS[name] = mh


def run_named_entry_handler(name: str, *args: str) -> None | Exception:
    return ENTRY_HANDLERS[name](*args)
