import json

from thds.mops.pure.core import lease

_MEMO_URI_SUFFIX = "mops2-mpf/pipe/pkg.mod--fn/hash123"


def _write_lease(tmp_path, **contents):
    lease_dir = tmp_path / _MEMO_URI_SUFFIX / lease.LEASE_DIRNAME
    lease_dir.mkdir(parents=True, exist_ok=True)
    (lease_dir / "lock.json").write_text(
        json.dumps(
            {
                "writer_id": "Writer.abc",
                "written_at": "2026-08-07T12:00:00+00:00",
                "expire_s": 88.0,
                **contents,
            }
        )
    )


def test_the_lease_uri_is_derived_from_the_memo_uri(tmp_path):
    """No lookup and nothing recorded - a caller holding a memo uri can always find the
    lease that goes with it."""
    memo_uri = f"file://{tmp_path}/{_MEMO_URI_SUFFIX}"

    assert lease.lease_uri_for(memo_uri) == f"{memo_uri}/{lease.LEASE_DIRNAME}/lock.json"


def test_reading_a_held_lease(tmp_path):
    _write_lease(tmp_path, writer_id="Writer.xyz")

    contents = lease.read_lease(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}")

    assert contents is not None
    assert contents["writer_id"] == "Writer.xyz"
    assert contents["expire_s"] == 88.0


def test_an_invocation_with_no_lease_reads_as_none(tmp_path):
    """Nothing ever worked on it, or the lease was cleaned up. Not an error."""
    assert lease.read_lease(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}") is None


def test_an_unreadable_lease_does_not_raise(tmp_path):
    """A caller diagnosing a stuck invocation should be told 'no information', not handed
    an exception."""
    assert lease.read_lease("not-a-uri-at-all") is None
