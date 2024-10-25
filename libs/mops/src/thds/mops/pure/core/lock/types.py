import typing as ty


class LockContents(ty.TypedDict):
    """Only writer_id, written_at, and expire are technically required for the algorithm
    - everything else is debugging info.

    In fact, expire_s would be 'optional' as well (this can be acquirer-only state), but
    it is advantegous to embed this explicitly, partly so that we can have remote
    'maintainers' that do not need to have any information other than the lock uri passed
    to them in order to maintain the lock.
    """

    writer_id: str
    written_at: str  # ISO8601 string with timezone in UTC
    expire_s: float  # seconds after written_at to expire

    # just for debugging
    hostname: str
    pid: str
    write_count: int
    first_written_at: str
    first_acquired_at: str
    released_at: str


class LockAcquired(ty.Protocol):

    writer_id: str

    def maintain(self) -> None:
        ...  # pragma: no cover

    def release(self) -> None:
        ...  # pragma: no cover

    expire_s: float
