import threading


class Counter:
    def __init__(self) -> None:
        self.value = 0
        self._lock = threading.Lock()

    def inc(self) -> int:
        with self._lock:
            self.value += 1
            return self.value


LAUNCH_COUNT = Counter()
FINISH_COUNT = Counter()
