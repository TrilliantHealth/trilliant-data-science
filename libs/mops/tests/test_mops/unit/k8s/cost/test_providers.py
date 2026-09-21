import datetime as dt

from kubernetes import client

from thds.mops.k8s.cost import model, providers


def _classify(raw: client.V1Node):
    name, uid, created = model.identity(raw)
    return model.Node(name, uid, "example", "workers", "large", "moon-1", "leased", created)


def _price(node: model.Node):
    return model.Price(1.25, "USD", "example-price-list")


def example_provider():
    return model.Provider("example", _classify, _price)


def _node() -> client.V1Node:
    return client.V1Node(
        metadata=client.V1ObjectMeta(
            name="node-1",
            uid="uid-1",
            creation_timestamp=dt.datetime(2026, 9, 19, tzinfo=dt.timezone.utc),
        )
    )


def test_dotted_factory_supplies_an_external_provider():
    resolver = providers.resolve(f"{__name__}.example_provider")

    node = providers.node(resolver, _node())

    assert node.provider == "example"
    assert providers.price(resolver, node).hourly_rate == 1.25


def test_unmatched_node_remains_visible_as_unpriced():
    resolver: providers.Resolver = ()

    node = providers.node(resolver, _node())

    assert node.provider == "unknown"
    assert providers.price(resolver, node).hourly_rate is None
