import datetime as dt

from thds.mops.k8s.cost import ledger, model


class _BlobStore:
    def __init__(self):
        self.writes = []

    def join(self, *parts):
        return "/".join(parts)

    def putbytes(self, uri, data, **_):
        self.writes.append((uri, data))


def test_new_remote_root_receives_the_whole_local_ledger(monkeypatch, tmp_path):
    blob = _BlobStore()
    roots = ["root-a"]
    monkeypatch.setattr(ledger.uris, "lookup_blob_store", lambda _: blob)
    writer = ledger.create(tmp_path, lambda: roots)
    initial = writer
    at = dt.datetime(2026, 9, 19, tzinfo=dt.timezone.utc)

    for offset in range(3):
        writer = ledger.append(
            writer,
            [
                ledger.heartbeat(
                    "observer",
                    at + dt.timedelta(seconds=offset),
                    at + dt.timedelta(seconds=offset + 1),
                    "cluster",
                    "namespace",
                    10,
                )
            ],
        )
        if offset == 1:
            roots.append("root-b")

    writes_a = [data for uri, data in blob.writes if uri.startswith("root-a/")]
    writes_b = [data for uri, data in blob.writes if uri.startswith("root-b/")]
    assert [data.count(b"\n") for data in writes_a] == [1, 1, 1]
    assert [data.count(b"\n") for data in writes_b] == [3]
    assert initial.offsets == ()
    assert dict(writer.offsets).keys() == {"root-a", "root-b"}


def test_node_interval_stores_provider_neutral_billing_dimensions():
    at = dt.datetime(2026, 9, 19, tzinfo=dt.timezone.utc)
    node = model.Node(
        "node-1",
        "uid-1",
        "example",
        "workers",
        "large",
        "moon-1",
        "leased",
        at,
    )

    observation = ledger.node_interval(
        "observer",
        at,
        at + dt.timedelta(seconds=10),
        "cluster",
        "namespace",
        node,
        model.Price(1.25, "USD", "example-price-list", hourly_rate_candidates=(1.0, 1.25)),
    )

    assert observation["provider"] == "example"
    assert observation["allocation_group"] == "workers"
    assert observation["instance_type"] == "large"
    assert observation["purchase_option"] == "leased"
    assert observation["hourly_rate_candidates"] == [1.0, 1.25]
