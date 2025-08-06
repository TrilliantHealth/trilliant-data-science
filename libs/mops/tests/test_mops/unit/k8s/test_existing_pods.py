import subprocess
from unittest.mock import Mock, patch

import pytest

from thds.mops.k8s.orchestrator.existing_pods import CreatePod, connect_if_exists


def create_mock_pod(
    name: str, image: str = "test:latest", phase: str = "Running", namespace: str = "test-ns"
):
    """Helper to create minimal mock pod objects."""
    pod = Mock()
    pod.metadata.name = name
    pod.metadata.namespace = namespace
    pod.spec.containers = [Mock(image=image)]
    pod.status.phase = phase
    return pod


def not_called(name: str = "This function"):
    """Mock that raises if called - use when a function should not be invoked."""

    def _raise(*args, **kwargs):
        raise AssertionError(f"{name} should not have been called")

    return Mock(side_effect=_raise)


# Happy path tests


@patch(
    "thds.mops.k8s.orchestrator.existing_pods.default_orch_pod_name",
    return_value="orch-myproject-abc123",
)
@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called())
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called())
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_when_nothing_exists_resolve_image_then_invent_name_then_create(
    mock_yield_pods, mock_default_name
):
    """User creates a new pod when no existing pods are found."""
    mock_yield_pods.return_value = []

    result = connect_if_exists(default_image_resolver=lambda: "myimage:v1.0", namespace="test-ns")
    assert result == CreatePod("test-ns", "orch-myproject-abc123", "myimage:v1.0")
    mock_yield_pods.assert_called_once_with("test-ns")
    assert mock_default_name.call_count == 1


@patch("thds.mops.k8s.orchestrator.existing_pods.connect")
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod")
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods", not_called())
def test_connect_to_existing_running_pod_by_name(mock_get_pod, mock_connect):
    """User connects to existing running pod successfully by name."""
    mock_pod = create_mock_pod("my-app-123")
    mock_get_pod.return_value = mock_pod

    result = connect_if_exists(image="", pod_name="my-app-123", namespace="test-ns", unsafe=True)

    mock_get_pod.assert_called_once_with("my-app-123", "test-ns")
    mock_connect.assert_called_once_with("my-app-123", "bash", namespace="test-ns")
    assert result is None


@patch("thds.mops.k8s.orchestrator.existing_pods.connect")
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_connect_to_existing_pod_by_image_and_name_match(mock_yield, mock_connect):
    """User connects to existing pod that matches both image and name."""
    mock_pod = create_mock_pod("orch-myproject-abc123", "myimage:v1.0")
    mock_yield.return_value = [mock_pod]

    result = connect_if_exists(
        image="myimage:v1.0", pod_name="orch-myproject-abc123", namespace="test-ns"
    )

    mock_yield.assert_called_once_with("test-ns")
    mock_connect.assert_called_once_with("orch-myproject-abc123", "bash", namespace="test-ns")
    assert result is None


@patch(
    "thds.mops.k8s.orchestrator.existing_pods.default_orch_pod_name",
    return_value="orch-myproject-abc123",
)
@patch("thds.mops.k8s.orchestrator.existing_pods.connect")
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod")
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods", not_called())
def test_connect_using_default_pod_name_generation(mock_get_pod, mock_connect, mock_default_name):
    """User connects using auto-generated pod name when none provided and unsafe is True."""
    mock_pod = create_mock_pod("orch-myproject-abc123")
    mock_get_pod.return_value = mock_pod

    result = connect_if_exists(image="", project_name="myproject", namespace="test-ns", unsafe=True)

    mock_default_name.assert_called_once_with("myproject")
    mock_get_pod.assert_called_once_with("orch-myproject-abc123", "test-ns")
    mock_connect.assert_called_once_with("orch-myproject-abc123", "bash", namespace="test-ns")
    assert result is None


@patch("thds.mops.k8s.orchestrator.existing_pods.connect")
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called())
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_find_pod_by_image_and_pod_name_prefix(mock_yield_pods, mock_connect):
    """User connects to existing pod that matches image and pod name prefix."""
    mock_pod = create_mock_pod("orch-myproject-abc123", "myimage:v1.0")
    mock_yield_pods.return_value = [mock_pod]

    result = connect_if_exists(image="myimage:v1.0", pod_name="orch-myproject", namespace="test-ns")

    mock_yield_pods.assert_called_once_with("test-ns")
    mock_connect.assert_called_once_with("orch-myproject-abc123", "bash", namespace="test-ns")
    assert result is None


# Pod phase handling tests


@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod")
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_failed_pod_gets_deleted_new_pod_needed(mock_yield, mock_delete):
    """Failed pod gets deleted and new pod creation is requested."""
    mock_pod = create_mock_pod("test-pod-123", "myimage:v1.0", phase="Failed")
    mock_yield.return_value = [mock_pod]

    result = connect_if_exists(image="myimage:v1.0", pod_name="test-pod", namespace="test-ns")

    mock_yield.assert_called_once_with("test-ns")
    mock_delete.assert_called_once_with("test-pod-123", namespace="test-ns")
    assert result == CreatePod("test-ns", "test-pod", "myimage:v1.0")


@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod")
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_succeeded_pod_gets_deleted_new_pod_needed(mock_yield, mock_delete):
    """Succeeded pod gets deleted and new pod creation is requested."""
    mock_pod = create_mock_pod("test-pod-123", "myimage:v1.0", phase="Succeeded")
    mock_yield.return_value = [mock_pod]

    result = connect_if_exists(image="myimage:v1.0", pod_name="test-pod", namespace="test-ns")

    mock_yield.assert_called_once_with("test-ns")
    mock_delete.assert_called_once_with("test-pod-123", namespace="test-ns")
    assert result == CreatePod("test-ns", "test-pod", "myimage:v1.0")


@pytest.mark.parametrize("phase", ["Unknown", "Terminating", "Pending"])
@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_transition_state_pod_raises_error(mock_yield, phase):
    """Pod in transition state raises ValueError."""
    mock_pod = create_mock_pod("test-pod-123", "myimage:v1.0", phase=phase)
    mock_yield.return_value = [mock_pod]

    with pytest.raises(ValueError, match="is in a transition state - aborting"):
        connect_if_exists(image="myimage:v1.0", pod_name="test-pod", namespace="test-ns")

    mock_yield.assert_called_once_with("test-ns")


@patch("thds.mops.k8s.orchestrator.existing_pods.input", return_value="y")
@patch("thds.mops.k8s.orchestrator.existing_pods.connect")
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_connection_fails_user_chooses_to_start_new_pod(mock_yield, mock_connect, mock_input):
    """Connection fails, user chooses to start new pod."""
    mock_pod = create_mock_pod("test-pod-123", "myimage:v1.0")
    mock_yield.return_value = [mock_pod]
    mock_connect.side_effect = subprocess.CalledProcessError(1, "kubectl")

    result = connect_if_exists(image="myimage:v1.0", pod_name="test-pod", namespace="test-ns")

    mock_yield.assert_called_once_with("test-ns")
    mock_connect.assert_called_once_with("test-pod-123", "bash", namespace="test-ns")
    mock_input.assert_called_once_with(
        "Pod test-pod-123 is not running or your command exited prematurely - attempt to start? [Y/n] "
    )
    assert result == CreatePod("test-ns", "test-pod", "myimage:v1.0")


@patch("thds.mops.k8s.orchestrator.existing_pods.input", return_value="n")
@patch("thds.mops.k8s.orchestrator.existing_pods.connect")
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_connection_fails_user_refuses_to_start(mock_yield, mock_connect, mock_input):
    """Connection fails, user refuses to start new pod."""
    mock_pod = create_mock_pod("test-pod-123", "myimage:v1.0")
    mock_yield.return_value = [mock_pod]
    mock_connect.side_effect = subprocess.CalledProcessError(1, "kubectl")

    result = connect_if_exists(image="myimage:v1.0", pod_name="test-pod", namespace="test-ns")

    mock_yield.assert_called_once_with("test-ns")
    mock_connect.assert_called_once_with("test-pod-123", "bash", namespace="test-ns")
    mock_input.assert_called_once_with(
        "Pod test-pod-123 is not running or your command exited prematurely - attempt to start? [Y/n] "
    )
    assert result is None


@patch("thds.mops.k8s.orchestrator.existing_pods.connect")
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_connection_fails_with_non_recoverable_error(mock_yield, mock_connect):
    """Connection fails with non-recoverable error code."""
    mock_pod = create_mock_pod("test-pod-123", "myimage:v1.0")
    mock_yield.return_value = [mock_pod]
    mock_connect.side_effect = subprocess.CalledProcessError(2, "kubectl")

    with pytest.raises(subprocess.CalledProcessError):
        connect_if_exists(image="myimage:v1.0", pod_name="test-pod", namespace="test-ns")

    mock_yield.assert_called_once_with("test-ns")
    mock_connect.assert_called_once_with("test-pod-123", "bash", namespace="test-ns")


# Image resolution tests


@patch("thds.mops.k8s.orchestrator.existing_pods.input", return_value="resolved:v1.0")
@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_image_resolved_from_user_input(mock_yield, mock_input):
    """Image gets resolved from user input when needed."""
    mock_yield.return_value = []  # No pods found
    default_resolver = Mock(return_value="")

    result = connect_if_exists(
        command="bash",
        image="",
        default_image_resolver=default_resolver,
        pod_name="test-pod",
        namespace="test-ns",
    )

    default_resolver.assert_called_once()
    mock_input.assert_called_once_with("\nA fully-qualified container registry image tag is required: ")
    mock_yield.assert_called_once_with("test-ns")
    assert result == CreatePod("test-ns", "test-pod", "resolved:v1.0")


# Dirty image and unsafe flag tests


@patch("thds.mops.k8s.orchestrator.existing_pods.input", return_value="n")
def test_dirty_image_without_unsafe_flag_raises_error(mock_input):
    """Dirty image without unsafe flag raises ValueError."""

    with pytest.raises(
        ValueError, match="You must provide a non-dirty image ref if you do not use --unsafe"
    ):
        connect_if_exists(
            command="bash",
            image="myimage:v1.0-dirty",
            pod_name="test-pod",
            namespace="test-ns",
            unsafe=False,
        )


@patch("thds.mops.k8s.orchestrator.existing_pods.input", return_value="y")
@patch("thds.mops.k8s.orchestrator.existing_pods.connect")
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_dirty_image_with_unsafe_flag_proceeds(mock_yield, mock_connect, mock_input):
    """Dirty image with unsafe flag proceeds after user confirmation."""
    mock_pod = create_mock_pod("test-pod-123", "myimage:v1.0-dirty")
    mock_yield.return_value = [mock_pod]

    result = connect_if_exists(
        image="myimage:v1.0-dirty", pod_name="test-pod", namespace="test-ns", unsafe=True
    )

    mock_input.assert_called_once_with(
        "Are you sure you want to connect to a pod with a dirty image? [y/N] "
    )
    mock_yield.assert_called_once_with("test-ns")
    mock_connect.assert_called_once_with("test-pod-123", "bash", namespace="test-ns")
    assert result is None


# Print pod name tests


@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
@patch("builtins.print")
def test_print_pod_name_found_by_image(mock_print, mock_yield):
    """When print_pod_name=True and pod found, prints name and returns None."""
    mock_pod = create_mock_pod("orch-myproject-abc123", "myimage:v1.0")
    mock_yield.return_value = [mock_pod]

    result = connect_if_exists(
        command="bash",
        image="myimage:v1.0",
        pod_name="orch-myproject",
        namespace="test-ns",
        print_pod_name=True,
    )

    mock_yield.assert_called_once_with("test-ns")
    mock_print.assert_called_once_with("orch-myproject-abc123")
    assert result is None


# Dry run tests


@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_dry_run_with_existing_pod(mock_yield):
    """Dry run with existing pod returns CreatePod without connecting."""
    mock_pod = create_mock_pod("test-pod-123", "myimage:v1.0")
    mock_yield.return_value = [mock_pod]

    result = connect_if_exists(
        image="myimage:v1.0", pod_name="test-pod", namespace="test-ns", dry_run=True
    )

    mock_yield.assert_called_once_with("test-ns")
    assert result == CreatePod("test-ns", "test-pod", "myimage:v1.0")


@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_dry_run_no_pod_found(mock_yield):
    """Dry run with no existing pod returns CreatePod."""
    mock_yield.return_value = []

    result = connect_if_exists(
        image="myimage:v1.0", pod_name="test-pod", namespace="test-ns", dry_run=True
    )

    mock_yield.assert_called_once_with("test-ns")
    assert result == CreatePod("test-ns", "test-pod", "myimage:v1.0")


# ValueError scenarios from _find_image_match_pod


@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_multiple_image_matches_no_name_match_raises_error(mock_yield):
    """Multiple pods match image but none match name raises ValueError."""
    pods = [
        create_mock_pod("other-app-1", "myimage:v1.0"),
        create_mock_pod("other-app-2", "myimage:v1.0"),
    ]
    mock_yield.return_value = pods

    with pytest.raises(ValueError, match="matching image myimage:v1.0 but none with name match my-app"):
        connect_if_exists(image="myimage:v1.0", pod_name="my-app", namespace="test-ns")

    mock_yield.assert_called_once_with("test-ns")


@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_name_match_but_no_image_match_raises_error(mock_yield):
    """Pods match name but not image raises ValueError."""
    pods = [create_mock_pod("my-app-123", "different:v1.0")]
    mock_yield.return_value = pods

    with pytest.raises(ValueError, match="matching name my-app but none with image myimage:v1.0"):
        connect_if_exists(image="myimage:v1.0", pod_name="my-app", namespace="test-ns")

    mock_yield.assert_called_once_with("test-ns")


@patch("thds.mops.k8s.orchestrator.existing_pods.connect", not_called("connect"))
@patch("thds.mops.k8s.orchestrator.existing_pods.delete_pod", not_called("delete_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._get_pod", not_called("_get_pod"))
@patch("thds.mops.k8s.orchestrator.existing_pods._yield_pods")
def test_multiple_both_matches_raises_error(mock_yield):
    """Multiple pods match both image and name raises ValueError."""
    pods = [create_mock_pod("my-app-1", "myimage:v1.0"), create_mock_pod("my-app-2", "myimage:v1.0")]
    mock_yield.return_value = pods

    with pytest.raises(ValueError, match="matching image myimage:v1.0 with name match my-app"):
        connect_if_exists(image="myimage:v1.0", pod_name="my-app", namespace="test-ns")

    mock_yield.assert_called_once_with("test-ns")
