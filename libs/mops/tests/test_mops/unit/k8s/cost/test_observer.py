import datetime as dt
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
