import datetime as dt
import time
from types import SimpleNamespace

from kubernetes import client

from thds.mops.k8s.cost import azure, labels, model, observer
from thds.mops.k8s.target import K8sTarget


def _node(name: str, pool: str) -> client.V1Node:
    return client.V1Node(
        metadata=client.V1ObjectMeta(
            name=name,
            uid=f"uid-{name}",
            creation_timestamp=dt.datetime(2026, 9, 19, tzinfo=dt.timezone.utc),
            labels={
                "kubernetes.azure.com/agentpool": pool,
                "node.kubernetes.io/instance-type": "Standard_D4s_v5",
                "topology.kubernetes.io/region": "eastus",
            },
        )
    )


def test_job_label_preserves_transform_inputs(monkeypatch):
    monkeypatch.setenv("THDS_MOPS_CONSOLE_RUN_NAME", "2026-09-19/mr.Example")
    job = client.V1Job(
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"existing": "yes"}, annotations={"a": "b"}),
                spec=client.V1PodSpec(containers=[], restart_policy="Never"),
            )
        )
    )

    labelled = labels.add_to(job)

    assert labelled.spec.template.metadata.labels == {
        "existing": "yes",
        labels.RUN_LABEL: labels.value("2026-09-19/mr.Example"),
    }
    assert labelled.spec.template.metadata.annotations == {
        "a": "b",
        labels.RUN_ANNOTATION: "2026-09-19/mr.Example",
    }
    assert job.spec.template.metadata.labels == {"existing": "yes"}
    assert job.spec.template.metadata.annotations == {"a": "b"}


def test_sample_learns_a_pool_and_charges_all_of_its_nodes():
    nodes = [_node("pool-a-1", "pool-a"), _node("pool-a-2", "pool-a"), _node("pool-b-1", "pool-b")]
    pod = client.V1Pod(spec=client.V1PodSpec(containers=[], node_name="pool-a-1"))
    api = SimpleNamespace(
        list_namespaced_pod=lambda **_: SimpleNamespace(items=[pod]),
        list_node=lambda: SimpleNamespace(items=nodes),
    )

    observed, groups = observer._sample(
        api,
        (azure.PROVIDER,),
        "namespace",
        "run",
        frozenset(),
    )

    assert groups == frozenset({("azure", "pool-a")})
    assert {node.name for node in observed} == {"pool-a-1", "pool-a-2"}


def test_observation_transition_preserves_its_input_state():
    at = dt.datetime(2026, 9, 19, tzinfo=dt.timezone.utc)
    state = observer.State(at, at)
    node = model.Node("node", "uid", "example", "workers", "large", "moon-1", "leased", at)

    advanced, observations = observer._observations(
        state,
        (node,),
        at + dt.timedelta(seconds=10),
        "observer",
        K8sTarget("context", "namespace"),
        10,
        lambda _: model.Price(1.25, "USD", "example"),
    )

    assert state == observer.State(at, at)
    assert advanced.previous == at + dt.timedelta(seconds=10)
    assert advanced.seen_nodes == frozenset({"uid"})
    assert len(advanced.prices) == 1
    assert [value["kind"] for value in observations] == ["heartbeat", "node"]


def _observe_until(monkeypatch, tmp_path, stop, poll_seconds=10.0, sample_seconds=0.15):
    """Drive the real `observe` loop with a sample that takes as long as a cluster call."""
    monkeypatch.setattr(observer, "api_client", lambda context: None)
    monkeypatch.setattr(observer.client, "CoreV1Api", lambda api_client: None)
    monkeypatch.setattr(observer.ledger, "append", lambda writer, observations: writer)

    def slow_sample(*_args):
        time.sleep(sample_seconds)
        return (), frozenset()

    monkeypatch.setattr(observer, "_sample", slow_sample)
    observer.observe(K8sTarget("ctx", "ns"), "console-run", tmp_path, stop, poll_seconds=poll_seconds)


def test_observe_exits_once_its_owner_is_gone(monkeypatch, tmp_path):
    """A stop that fires immediately must end the loop after one closing sample."""
    calls = 0

    def stop(_timeout: float) -> bool:
        nonlocal calls
        calls += 1
        if calls > 10:
            raise AssertionError("observe() kept looping after stop() asked it to finish")
        return True

    _observe_until(monkeypatch, tmp_path, stop)

    assert calls == 2, "expected the in-flight sample plus exactly one closing sample"


def test_observe_keeps_polling_while_its_owner_lives(monkeypatch, tmp_path):
    """The stop signal is also the sleep, so a live owner must keep the loop going."""
    calls = 0

    def stop(_timeout: float) -> bool:
        nonlocal calls
        calls += 1
        if calls > 10:
            raise AssertionError("observe() kept looping after stop() asked it to finish")
        return calls >= 3  # alive for two polls, then gone

    _observe_until(monkeypatch, tmp_path, stop)

    assert calls == 4, "two live polls, the stop that fired, then the closing sample"


def test_observe_takes_a_single_sample_when_given_no_interval(monkeypatch, tmp_path):
    """poll_seconds=0 means 'sample once and return', independent of the stop signal."""
    calls = 0

    def stop(_timeout: float) -> bool:
        nonlocal calls
        calls += 1
        if calls > 10:
            raise AssertionError("observe() kept looping with poll_seconds=0")
        return False

    _observe_until(monkeypatch, tmp_path, stop, poll_seconds=0.0)

    assert calls == 1
