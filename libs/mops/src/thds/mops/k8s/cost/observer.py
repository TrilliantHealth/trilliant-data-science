"""Poll Kubernetes nodes and write cost observations for ``mops-console``."""

import datetime as dt
import os
import typing as ty
import uuid
from pathlib import Path

from kubernetes import client

from thds.core import config, log
from thds.mops.pure.tools.console import writer

from ..auth import api_client
from ..target import K8sTarget
from . import labels, ledger, model, providers

logger = log.getLogger(__name__)

ENABLED = config.item("mops.k8s.cost.enabled", default=True, parse=config.tobool)
POLL_SECONDS = config.item("mops.k8s.cost.poll_seconds", default=10.0, parse=float)

PriceKey = tuple[str, str, str, str]
Stop = ty.Callable[[float], bool]


class State(ty.NamedTuple):
    started: dt.datetime
    previous: dt.datetime
    known_groups: frozenset[tuple[str, str]] = frozenset()
    seen_nodes: frozenset[str] = frozenset()
    prices: tuple[tuple[PriceKey, model.Price], ...] = ()


def _sample(
    api: client.CoreV1Api,
    resolver: providers.Resolver,
    namespace: str,
    run_label: str,
    known_groups: frozenset[tuple[str, str]],
) -> tuple[tuple[model.Node, ...], frozenset[tuple[str, str]]]:
    """Nodes chargeable to the run and accumulated provider allocation groups."""
    pods = api.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"{labels.RUN_LABEL}={run_label}",
    ).items
    nodes = tuple(providers.node(resolver, raw) for raw in api.list_node().items)
    by_name = {node.name: node for node in nodes}
    assigned = {
        node
        for pod in pods
        if (node_name := getattr(pod.spec, "node_name", ""))
        and (node := by_name.get(node_name)) is not None
    }
    groups = known_groups | frozenset(
        (node.provider, node.allocation_group) for node in assigned if node.allocation_group
    )
    participating = tuple(
        node
        for node in nodes
        if (node.allocation_group and (node.provider, node.allocation_group) in groups)
        or node in assigned
    )
    return participating, groups


def _observations(
    state: State,
    nodes: ty.Iterable[model.Node],
    now: dt.datetime,
    observer_id: str,
    target: K8sTarget,
    poll_seconds: float,
    price: ty.Callable[[model.Node], model.Price],
) -> tuple[State, tuple[ledger.Observation, ...]]:
    seen_nodes = set(state.seen_nodes)
    prices = dict(state.prices)
    observations = [
        ledger.heartbeat(
            observer_id,
            state.previous,
            now,
            target.kubeconfig_context,
            target.namespace,
            poll_seconds,
        )
    ]
    for node in nodes:
        interval_start = state.previous
        if node.uid not in seen_nodes:
            interval_start = max(state.started, node.created_at)
            seen_nodes.add(node.uid)
        price_key = (
            node.provider,
            node.region,
            node.instance_type,
            node.purchase_option,
        )
        if price_key not in prices:
            resolved = price(node)
            prices[price_key] = resolved
            if len(set(resolved.hourly_rate_candidates)) > 1:
                logger.warning(
                    "Multiple hourly prices for %s %s in %s: %s; recording the full range.",
                    node.provider,
                    node.instance_type,
                    node.region,
                    resolved.hourly_rate_candidates,
                )
        observations.append(
            ledger.node_interval(
                observer_id,
                interval_start,
                now,
                target.kubeconfig_context,
                target.namespace,
                node,
                prices[price_key],
            )
        )
    return (
        state._replace(
            previous=now,
            seen_nodes=frozenset(seen_nodes),
            prices=tuple(prices.items()),
        ),
        tuple(observations),
    )


def observe(
    target: K8sTarget,
    console_run: str,
    run_dir: Path,
    stop: Stop,
    poll_seconds: float,
    configured_provider: str = "",
) -> None:
    observer_id = f"{os.getpid()}-{uuid.uuid4().hex[:12]}"
    started = dt.datetime.now(tz=dt.timezone.utc)
    state = State(started, started)
    resolver = providers.resolve(configured_provider)
    api = client.CoreV1Api(api_client=api_client(target.kubeconfig_context))
    cost_writer = ledger.create(run_dir, lambda: writer.remote_events_uris(run_dir))

    while True:
        now = dt.datetime.now(tz=dt.timezone.utc)
        try:
            nodes, known_groups = _sample(
                api,
                resolver,
                target.namespace,
                labels.value(console_run),
                state.known_groups,
            )
            state, observations = _observations(
                state._replace(known_groups=known_groups),
                nodes,
                now,
                observer_id,
                target,
                poll_seconds,
                lambda node: providers.price(resolver, node),
            )
            cost_writer = ledger.append(cost_writer, observations)
        except Exception:
            logger.debug(
                "Kubernetes cost observation failed; coverage will show the gap.", exc_info=True
            )
            state = state._replace(previous=now)

        if stop(poll_seconds):
            if dt.datetime.now(tz=dt.timezone.utc) - state.previous < dt.timedelta(seconds=0.1):
                break
            # Take one final sample so a normal orchestrator exit closes its coverage.
            poll_seconds = 0.0
            continue
        if poll_seconds == 0.0:
            break
