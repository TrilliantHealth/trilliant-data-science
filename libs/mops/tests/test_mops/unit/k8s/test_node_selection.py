from thds.mops.k8s.node_selection import require_gpu, require_node_labels


def test_require_node_labels_single_key_multiple_values():
    affinity = require_node_labels({"initiative": ["pool-a", "pool-b"]})
    terms = affinity.required_during_scheduling_ignored_during_execution.node_selector_terms
    assert len(terms) == 1
    exprs = terms[0].match_expressions
    assert len(exprs) == 1
    assert exprs[0].key == "initiative"
    assert exprs[0].operator == "In"
    assert exprs[0].values == ["pool-a", "pool-b"]


def test_require_node_labels_multiple_keys_are_anded():
    affinity = require_node_labels({"initiative": ["pool-a"], "instance-type": ["cpu", "gpu"]})
    terms = affinity.required_during_scheduling_ignored_during_execution.node_selector_terms
    assert len(terms) == 1
    exprs = {e.key: e for e in terms[0].match_expressions}
    assert exprs["initiative"].values == ["pool-a"]
    assert exprs["instance-type"].values == ["cpu", "gpu"]


def test_require_gpu_selects_the_plain_gpu_label_and_requests_no_device():
    # unchanged on purpose: existing callers depend on this exact narrowing
    narrowing = require_gpu()
    assert narrowing["node_selector"] == {"instance-type": "gpu"}
    assert "resource_requests" not in narrowing


def test_require_gpu_tolerates_the_dedicated_gpu_taint():
    (toleration,) = require_gpu()["tolerations"]
    assert (toleration.key, toleration.value) == ("dedicated", "gpu")
