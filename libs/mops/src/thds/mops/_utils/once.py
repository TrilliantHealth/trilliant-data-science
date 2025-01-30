import threading
import typing as ty

FNone = ty.TypeVar("FNone", bound=ty.Callable[[], None])


class Once:
    """Uses unique IDs to guarantee that an operation has only run
    once in the lifetime of this object, and waits for it to be complete.

    Is a potential source of memory leaks, since each event will be
    stored until the entire Once object is disposed.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.events: ty.Dict[ty.Hashable, threading.Event] = dict()

    def run_once(self, run_id: ty.Hashable, f: FNone) -> None:
        needs_run = False
        if run_id not in self.events:
            with self.lock:
                if run_id not in self.events:
                    needs_run = True
                    self.events[run_id] = threading.Event()
        if needs_run:
            f()
            self.events[run_id].set()
        else:
            self.events[run_id].wait()
