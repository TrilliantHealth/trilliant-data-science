"""Built-in Azure node classification and retail pricing."""

import json
import typing as ty
import urllib.parse
import urllib.request

from kubernetes import client

from . import model

_POOL_LABEL = "kubernetes.azure.com/agentpool"
_SKU_LABEL = "node.kubernetes.io/instance-type"
_REGION_LABEL = "topology.kubernetes.io/region"
_PRIORITY_LABEL = "kubernetes.azure.com/scalesetpriority"


class Price(ty.NamedTuple):
    hourly_rate: None | float
    error: str = ""
    candidates: tuple[float, ...] = ()


def retail_hourly(region: str, sku: str, spot: bool = False) -> Price:
    """Return the current Linux retail rate for one Azure VM SKU."""
    if not region or not sku:
        return Price(None, "node lacks Azure region or VM SKU labels")

    query = (
        f"serviceName eq 'Virtual Machines' and armRegionName eq '{region}' "
        f"and armSkuName eq '{sku}' and priceType eq 'Consumption'"
    )
    url = "https://prices.azure.com/api/retail/prices?" + urllib.parse.urlencode(
        {
            "api-version": "2023-01-01-preview",
            "meterRegion": "primary",
            "currencyCode": "'USD'",
            "$filter": query,
        }
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 - fixed HTTPS host
            document = json.load(response)
    except Exception as exc:
        return Price(None, f"Azure retail price lookup failed: {type(exc).__name__}")

    candidates = [
        item
        for item in document.get("Items", ())
        if item.get("type") == "Consumption"
        and item.get("isPrimaryMeterRegion") is True
        and item.get("unitOfMeasure") == "1 Hour"
        and "Windows" not in item.get("productName", "")
        and ("Spot" in item.get("meterName", "")) is spot
        and "Low Priority" not in item.get("meterName", "")
        and isinstance(item.get("retailPrice"), (int, float))
    ]
    rates = tuple(sorted({float(item["retailPrice"]) for item in candidates}))
    if not rates:
        return Price(None, f"no Linux PAYG retail price for {sku} in {region}")

    return Price(max(rates), candidates=rates)


def node(raw: client.V1Node) -> None | model.Node:
    """Classify an AKS node from its Kubernetes metadata."""
    labels = raw.metadata.labels or {}
    provider_id = getattr(raw.spec, "provider_id", "") or ""
    if _POOL_LABEL not in labels and not provider_id.lower().startswith("azure://"):
        return None
    name, uid, created = model.identity(raw)
    return model.Node(
        name=name,
        uid=uid,
        provider="azure",
        allocation_group=labels.get(_POOL_LABEL, ""),
        instance_type=labels.get(_SKU_LABEL, ""),
        region=labels.get(_REGION_LABEL, ""),
        purchase_option=("spot" if labels.get(_PRIORITY_LABEL, "").lower() == "spot" else "on-demand"),
        created_at=created,
    )


def price(node: model.Node) -> model.Price:
    """Price an Azure node from its frozen billing dimensions."""
    spot = node.purchase_option == "spot"
    retail = retail_hourly(node.region, node.instance_type, spot)
    return model.Price(
        hourly_rate=retail.hourly_rate,
        currency="USD",
        source="azure-retail-spot" if spot else "azure-retail-payg",
        error=retail.error,
        hourly_rate_candidates=retail.candidates,
    )


PROVIDER = model.Provider("azure", node, price)
