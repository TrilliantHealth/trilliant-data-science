"""Append-only local and remote storage for Kubernetes cost observations."""

import datetime as dt
import json
import os
import typing as ty
from pathlib import Path

from thds.core import log
from thds.mops.pure.core import uris

from . import model

logger = log.getLogger(__name__)


class Observation(ty.TypedDict, total=False):
    kind: ty.Literal["heartbeat", "node"]
    observed_from: str
    observed_through: str
    observer_id: str
    cluster: str
    namespace: str
    poll_seconds: float
    node_name: str
    node_uid: str
    provider: str
    allocation_group: str
    instance_type: str
    region: str
    purchase_option: str
    currency: str
    hourly_rate: float
    hourly_rate_candidates: list[float]
    price_source: str
    price_error: str


def heartbeat(
    observer_id: str,
    start: dt.datetime,
    end: dt.datetime,
    cluster: str,
    namespace: str,
    poll_seconds: float,
) -> Observation:
    return Observation(
        kind="heartbeat",
        observed_from=start.isoformat(),
        observed_through=end.isoformat(),
        observer_id=observer_id,
        cluster=cluster,
        namespace=namespace,
        poll_seconds=poll_seconds,
    )


def node_interval(
    observer_id: str,
    start: dt.datetime,
    end: dt.datetime,
    cluster: str,
    namespace: str,
    node: model.Node,
    price: model.Price,
) -> Observation:
    observation = Observation(
        kind="node",
        observed_from=start.isoformat(),
        observed_through=end.isoformat(),
        observer_id=observer_id,
        cluster=cluster,
        namespace=namespace,
        node_name=node.name,
        node_uid=node.uid,
        provider=node.provider,
        allocation_group=node.allocation_group,
        instance_type=node.instance_type,
        region=node.region,
        purchase_option=node.purchase_option,
        currency=price.currency,
        price_source=price.source,
    )
    if price.hourly_rate is not None:
        observation["hourly_rate"] = price.hourly_rate
    if price.hourly_rate_candidates:
        observation["hourly_rate_candidates"] = list(price.hourly_rate_candidates)
    if price.error:
        observation["price_error"] = price.error
    return observation


class Writer(ty.NamedTuple):
    """Immutable upload progress for one observer's append-only ledger."""

    path: Path
    roots: ty.Callable[[], ty.Iterable[str]]
    offsets: tuple[tuple[str, int], ...] = ()
    sequence: int = 0


def create(run_dir: Path, roots: ty.Callable[[], ty.Iterable[str]]) -> Writer:
    directory = run_dir / "costs"
    directory.mkdir(parents=True, exist_ok=True)
    return Writer(directory / f"observations-{os.getpid()}.jsonl", roots)


def _read_from(path: Path, offset: int) -> tuple[bytes, int]:
    with path.open("rb") as source:
        source.seek(offset)
        data = source.read()
    complete = data.rfind(b"\n") + 1
    return data[:complete], offset + complete


def _upload(writer: Writer) -> Writer:
    offsets = dict(writer.offsets)
    sequence = writer.sequence
    for root in dict.fromkeys(writer.roots()):
        try:
            data, offset = _read_from(writer.path, offsets.get(root, 0))
            if not data:
                continue
            stamp = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%dT%H%M%S%f")[:18]
            blob_store = uris.lookup_blob_store(root)
            blob_store.putbytes(
                blob_store.join(
                    root,
                    "costs",
                    f"{stamp}-observer-{os.getpid()}-{sequence}.jsonl",
                ),
                data,
                type_hint="application/mops-console-cost-observations",
            )
            offsets[root] = offset
            sequence += 1
        except Exception:
            logger.debug("Could not publish Kubernetes cost observations to %s", root, exc_info=True)
    return writer._replace(offsets=tuple(offsets.items()), sequence=sequence)


def append(writer: Writer, observations: ty.Iterable[Observation]) -> Writer:
    """Append observations and return the resulting per-root upload progress."""
    with writer.path.open("a", encoding="utf-8") as output:
        for observation in observations:
            output.write(json.dumps(observation) + "\n")
    return _upload(writer)
