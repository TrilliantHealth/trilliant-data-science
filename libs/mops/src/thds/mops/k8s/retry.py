import time
import typing as ty
from functools import wraps

import urllib3.exceptions
from kubernetes import client

from . import auth, config
from ._shared import logger

F = ty.TypeVar("F", bound=ty.Callable)


# The first thing you should know about the Kubernetes SDK is that it
# is riddled with race conditions and timeouts and all kinds of
# horrible gremlins.  This first function/decorator is an _ongoing_
# attempt to deal with the fallout from that.  Hopefully from now on
# we'll be able to consolidate/maintain all of that logic in a single
# place:


_URLLIB_COMMON = (
    urllib3.exceptions.ProtocolError,
    urllib3.exceptions.MaxRetryError,
)


def k8s_sdk_retry(
    get_retry_args_kwargs: ty.Optional[ty.Callable[[int], ty.Tuple[tuple, dict]]] = None,
    should_retry: ty.Callable[[Exception], bool] = lambda _: False,
    max_retries: int = 20,
) -> ty.Callable[[F], F]:
    """Handle the common cases - lets you decide about uncommon ones."""

    def decorator(f: F) -> F:
        @wraps(f)
        def wrapper(*args, **kwargs):  # type: ignore
            i = 0

            def _raise_if_max(i: int) -> None:
                if i >= max_retries:
                    logger.warning(f"Failing after {i} tries")
                    raise

            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as ex:
                    # some shared behavior for all exceptions means we want a single except block
                    _raise_if_max(i)
                    if isinstance(ex, _URLLIB_COMMON):
                        # these are extremely common and should always be retried
                        logger.debug(
                            "Encountered probable connection timeout - retrying",
                            exc=str(ex),
                        )
                        # necessary b/c https://github.com/kubernetes-client/python/issues/1234
                    elif isinstance(ex, client.exceptions.ApiException) and ex.reason == "Unauthorized":
                        # this one is fairly common - who knows why their SDK can't handle this automatically.
                        #
                        # https://github.com/kubernetes-client/python/blob/release-18.0/kubernetes/client/exceptions.py?ts=4#L84
                        logger.info(f"{ex} - retrying after auth failure")
                        auth.load_config()
                    elif not should_retry(ex):
                        raise

                    i += 1
                    logger.info(f"Will retry after K8S error {str(ex)}; attempt {i}")
                time.sleep(config.k8s_monitor_delay())
                if get_retry_args_kwargs:
                    args, kwargs = get_retry_args_kwargs(i)

        return ty.cast(F, wrapper)

    return decorator
