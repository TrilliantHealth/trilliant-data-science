from .types import Protocol, ty


class MainHandler(Protocol):
    def __call__(self, *__args: str) -> ty.Any:
        ...  # pragma: nocover


REMOTE_RUNNERS: ty.Dict[str, MainHandler] = dict()

MAIN_HANDLER_BASE_ARGS = ["python", "-m", "thds.mops.remote.main"]


def register_main_handler(name: str, mh: MainHandler):
    REMOTE_RUNNERS[name] = mh


def main_handler(name: str, *args: str):
    REMOTE_RUNNERS[name](*args)
