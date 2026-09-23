"""A BaseException that is not an Exception (here, an argparse-style SystemExit from an
in-process invocation) must still release the lease and settle every future waiting on
it. Otherwise lease maintenance refreshes the lease forever, and every later caller of
the same memo waits on it forever."""

import typing as ty
from datetime import datetime, timedelta

import pytest

from thds.mops import pure
from thds.mops.pure.core import lease
from thds.mops.pure.core.lease import maintain

from ...config import TEST_TMP_URI

_PIPELINE = f"test/base-exception-lease/{datetime.utcnow().isoformat()}"


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_PIPELINE)
def _exits_on_first_line(x: int) -> int:
    raise SystemExit(2)


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_PIPELINE)
def _exits_after_takeover(x: int) -> int:
    raise SystemExit(2)


def _record_acquisitions(
    monkeypatch: pytest.MonkeyPatch,
    acquire: ty.Callable[..., ty.Optional[lease.LeaseAcquired]],
) -> list[tuple[str, lease.LeaseAcquired]]:
    acquired: list[tuple[str, lease.LeaseAcquired]] = []

    def recording_acquire(lease_dir_uri: str, **kwargs: ty.Any) -> ty.Optional[lease.LeaseAcquired]:
        lease_owned = acquire(lease_dir_uri, **kwargs)
        if lease_owned is not None:
            acquired.append((lease_dir_uri, lease_owned))
        return lease_owned

    monkeypatch.setattr(lease, "acquire", recording_acquire)
    return acquired


def _assert_released_and_unmaintained(
    acquired: ty.Sequence[tuple[str, lease.LeaseAcquired]],
) -> None:
    for _, lease_owned in acquired:
        assert lease_owned.writer_id not in maintain._LEASE_RELEASERS_BY_ID, "lease is still maintained"

    lease_dir_uri, lease_owned = acquired[-1]
    contents = lease.read_lease(lease_dir_uri.removesuffix("/" + lease.LEASE_DIRNAME))
    assert contents is not None
    assert contents["writer_id"] == lease_owned.writer_id
    assert contents.get("released_at"), f"lease was never released: {contents}"


def test_system_exit_from_the_shim_releases_the_lease(monkeypatch):
    acquired = _record_acquisitions(monkeypatch, lease.acquire)

    with pytest.raises(SystemExit):
        _exits_on_first_line(1)

    assert len(acquired) == 1
    _assert_released_and_unmaintained(acquired)

    # a released lease is acquired immediately, so a retry fails fast instead of waiting.
    with pytest.raises(SystemExit):
        _exits_on_first_line.submit(1).result(timeout=30)


def test_system_exit_after_a_takeover_releases_the_lease_and_fails_every_waiter(monkeypatch):
    # The first acquisition stands in for another process that acquired the lease and
    # then vanished without maintaining or releasing it. Every lease is short so the
    # abandoned one expires within a few waiter polls; the takeover's own lease is
    # maintained normally.
    real_acquire = lease.acquire
    abandoned: list[lease.LeaseAcquired] = []

    def acquire_behind_an_abandoned_lease(
        lease_dir_uri: str, **_kwargs: ty.Any
    ) -> ty.Optional[lease.LeaseAcquired]:
        lease_owned = real_acquire(lease_dir_uri, expire=timedelta(seconds=2))
        if not abandoned and lease_owned is not None:
            abandoned.append(lease_owned)
            return None

        return lease_owned

    acquired = _record_acquisitions(monkeypatch, acquire_behind_an_abandoned_lease)

    first = _exits_after_takeover.submit(3)
    second = _exits_after_takeover.submit(3)
    assert not first.done() and not second.done()  # both genuinely lease-blocked

    for waiter in (first, second):
        with pytest.raises(SystemExit):
            waiter.result(timeout=60)

    assert acquired, "no waiter ever took over the abandoned lease"
    _assert_released_and_unmaintained(acquired)
