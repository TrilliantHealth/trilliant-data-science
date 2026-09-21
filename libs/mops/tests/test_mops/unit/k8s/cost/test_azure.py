import datetime as dt
import json

from kubernetes import client

from thds.mops.k8s.cost import azure


class _Response:
    def __init__(self, document):
        self._raw = json.dumps(document).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def read(self):
        return self._raw


def test_retail_hourly_selects_linux_payg_and_excludes_spot(monkeypatch):
    items = [
        {
            "type": "Consumption",
            "isPrimaryMeterRegion": True,
            "unitOfMeasure": "1 Hour",
            "productName": "Virtual Machines Dsv5 Series Windows",
            "meterName": "D4s v5",
            "retailPrice": 0.40,
        },
        {
            "type": "Consumption",
            "isPrimaryMeterRegion": True,
            "unitOfMeasure": "1 Hour",
            "productName": "Virtual Machines Dsv5 Series",
            "meterName": "D4s v5 Spot",
            "retailPrice": 0.05,
        },
        {
            "type": "Consumption",
            "isPrimaryMeterRegion": True,
            "unitOfMeasure": "1 Hour",
            "productName": "Virtual Machines Dsv5 Series",
            "meterName": "D4s v5",
            "retailPrice": 0.20,
        },
    ]
    monkeypatch.setattr(
        azure.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response({"Items": items}),
    )

    assert azure.retail_hourly("eastus", "Standard_D4s_v5") == azure.Price(0.20, candidates=(0.20,))
    assert azure.retail_hourly("eastus", "Standard_D4s_v5", spot=True) == azure.Price(
        0.05, candidates=(0.05,)
    )


def test_provider_classifies_an_aks_spot_node():
    raw = client.V1Node(
        metadata=client.V1ObjectMeta(
            name="node-1",
            uid="uid-1",
            creation_timestamp=dt.datetime(2026, 9, 19, tzinfo=dt.timezone.utc),
            labels={
                "kubernetes.azure.com/agentpool": "pool-a",
                "kubernetes.azure.com/scalesetpriority": "spot",
                "node.kubernetes.io/instance-type": "Standard_D4s_v5",
                "topology.kubernetes.io/region": "eastus",
            },
        )
    )

    node = azure.PROVIDER.node(raw)

    assert node is not None
    assert node.provider == "azure"
    assert node.allocation_group == "pool-a"
    assert node.purchase_option == "spot"


def test_retail_hourly_preserves_ambiguous_prices(monkeypatch):
    items = [
        {
            "type": "Consumption",
            "isPrimaryMeterRegion": True,
            "unitOfMeasure": "1 Hour",
            "productName": "Virtual Machines Dsv5 Series",
            "meterName": "D4s v5",
            "retailPrice": price,
        }
        for price in (0.19, 0.20)
    ]
    monkeypatch.setattr(
        azure.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response({"Items": items}),
    )

    assert azure.retail_hourly("eastus", "Standard_D4s_v5") == azure.Price(0.20, candidates=(0.19, 0.20))
