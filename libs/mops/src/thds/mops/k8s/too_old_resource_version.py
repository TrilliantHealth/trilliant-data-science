import re
import typing as ty

from kubernetes import client

_TOO_OLD_RESOURCE_VERSION = re.compile(
    r"Expired: too old resource version: (?P<old>\w+) \((?P<cur>\w+)\)"
)
# holy bananas I cannot believe how much K8s' SDK sucks.  this is a
# standard exception with an known retry semantic that their watchers
# are apparently unable to handle on their own - I'm staring at their
# code right now and they don't even attempt to handle this.


class TooOldResourceVersion(ty.NamedTuple):
    old: str
    cur: str
    spread: str  # only if the above are actually numbers


def parse_too_old_resource_version(
    exc: Exception,
) -> ty.Optional[TooOldResourceVersion]:
    if not isinstance(exc, client.exceptions.ApiException):
        return None
    m = _TOO_OLD_RESOURCE_VERSION.match(exc.reason)
    if m:
        # this is a completely bonkers thing to have to do
        # ourselves, but here we are.  I can't find any
        # documentation on why their SDK doesn't handle this
        # themselves, and I don't even know why we haven't run
        # into it before. Regardless, apparently we have to
        # special-case a retry when there are enough old
        # events on the server.
        resource_version = m.group("cur")
        old = m.group("old")
        try:
            spread = str(int(resource_version) - int(m.group("old")))
        except ValueError:
            spread = "unknown"
        return TooOldResourceVersion(old, resource_version, spread)
    return None
