from unittest.mock import Mock, patch

import pytest

from thds.mops.k8s.orchestrator.connect_or_start import _confirm_dirty_image, find_pod


def create_mock_pod(name: str, image: str = "test:latest", phase: str = "Running"):
    """Helper to create minimal mock pod objects."""
    pod = Mock()
    pod.metadata.name = name
    pod.metadata.namespace = "test-ns"
    pod.spec.containers = [Mock(image=image)]
    pod.status.phase = phase
    return pod


def not_called():
    """Mock that raises if called - use when a function should not be invoked."""

    def _raise(*args, **kwargs):
        raise AssertionError("This function should not have been called")

    return Mock(side_effect=_raise)


@patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods")
def test_find_pod_successful_image_and_name_match(mock_yield):
    """Test happy path: pod matches both image and name."""
    mock_pod = create_mock_pod("my-app-123", "myimage:v1.0")
    mock_yield.return_value = [mock_pod]

    result = find_pod("my-app", namespace="test-ns", only_this_image="myimage:v1.0")

    mock_yield.assert_called_once_with("test-ns")
    assert result == mock_pod


@patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod")
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods", not_called())
def test_find_pod_fallback_to_name_only(mock_get_pod):
    """Test fallback to name lookup when no image specified."""
    mock_pod = create_mock_pod("test-pod")
    mock_get_pod.return_value = mock_pod

    result = find_pod("test-pod", namespace="test-ns", only_this_image="")

    mock_get_pod.assert_called_once_with("test-pod", "test-ns")
    assert result == mock_pod


@patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod")
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods", not_called())
def test_find_pod_not_found(mock_get_pod):
    """Test when pod is not found by either method."""
    mock_get_pod.return_value = None

    result = find_pod("nonexistent", namespace="test-ns", only_this_image="")

    mock_get_pod.assert_called_once_with("nonexistent", "test-ns")
    assert result is None


@patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods")
def test_find_pod_multiple_image_matches_no_name_match(mock_yield):
    """Test error when multiple pods match image but none match name."""
    pods = [
        create_mock_pod("other-app-1", "myimage:v1.0"),
        create_mock_pod("other-app-2", "myimage:v1.0"),
    ]
    mock_yield.return_value = pods

    with pytest.raises(ValueError, match="matching image myimage:v1.0 but none with name match my-app"):
        find_pod("my-app", namespace="test-ns", only_this_image="myimage:v1.0")

    mock_yield.assert_called_once_with("test-ns")


@patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods")
def test_find_pod_no_image_matches(mock_yield):
    """Test error when no pods match the specified image."""
    pods = [create_mock_pod("my-app-123", "different:v1.0")]
    mock_yield.return_value = pods

    with pytest.raises(ValueError, match="but none with image myimage:v1.0"):
        find_pod("my-app", namespace="test-ns", only_this_image="myimage:v1.0")

    mock_yield.assert_called_once_with("test-ns")


@patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods")
def test_find_pod_multiple_both_matches(mock_yield):
    """Test error when multiple pods match both image and name."""
    pods = [create_mock_pod("my-app-1", "myimage:v1.0"), create_mock_pod("my-app-2", "myimage:v1.0")]
    mock_yield.return_value = pods

    with pytest.raises(ValueError, match="matching image myimage:v1.0 with name match my-app"):
        find_pod("my-app", namespace="test-ns", only_this_image="myimage:v1.0")

    mock_yield.assert_called_once_with("test-ns")


@patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods")
def test_find_pod_name_match_but_no_image_match(mock_yield):
    """Test error when pods match name but not image."""
    pods = [create_mock_pod("my-app-123", "different:v1.0")]
    mock_yield.return_value = pods

    with pytest.raises(ValueError, match="matching name my-app but none with image myimage:v1.0"):
        find_pod("my-app", namespace="test-ns", only_this_image="myimage:v1.0")

    mock_yield.assert_called_once_with("test-ns")


@pytest.mark.parametrize(
    "phase,should_delete",
    [
        ("Succeeded", True),
        ("Failed", True),
        ("Running", False),
    ],
)
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod")
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods", not_called())
def test_find_pod_handle_terminal_phases(mock_get_pod, phase, should_delete):
    """Test that pods in terminal phases get deleted."""
    mock_pod = create_mock_pod("test-pod", phase=phase)
    mock_get_pod.return_value = mock_pod

    with patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod") as mock_delete:
        find_pod("test-pod", namespace="test-ns", only_this_image="")

        mock_get_pod.assert_called_once_with("test-pod", "test-ns")
        if should_delete:
            mock_delete.assert_called_once_with("test-pod", namespace="test-ns")
        else:
            mock_delete.assert_not_called()


@pytest.mark.parametrize("phase", ["Unknown", "Terminating", "Pending"])
@patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod")
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods", not_called())
def test_find_pod_transition_state_error(mock_get_pod, phase):
    """Test error when pod is in transition state."""
    mock_pod = create_mock_pod("test-pod", phase=phase)
    mock_get_pod.return_value = mock_pod

    with pytest.raises(ValueError, match="is in a transition state"):
        find_pod("test-pod", namespace="test-ns", only_this_image="")

    mock_get_pod.assert_called_once_with("test-pod", "test-ns")


@patch("thds.mops.k8s.orchestrator.connect_or_start.input", return_value="n")
@patch("thds.mops.k8s.orchestrator.connect_or_start.delete_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._get_pod", not_called())
@patch("thds.mops.k8s.orchestrator.connect_or_start._yield_pods", not_called())
def test_find_pod_dirty_image_user_refuses(mock_input):
    """Test that dirty image is detected and user can refuse to proceed."""
    with pytest.raises(ValueError, match="Thanks for refusing to connect to a pod with a dirty image"):
        find_pod("test-pod", namespace="test-ns", only_this_image="myimage:v1.0-dirty")

    mock_input.assert_called_once()


@pytest.mark.parametrize(
    "user_input",
    ["y", "Y", "yes", "Yes", "YES", "yep", "yeah", "yup"],
)
@patch("thds.mops.k8s.orchestrator.connect_or_start.logger")
def test_check_matching_image_dirty_image_user_confirms(mock_logger, user_input):
    """Should proceed with warning when user confirms dirty image connection."""
    with patch("builtins.input", return_value=user_input):
        _confirm_dirty_image("dirty-image-dirty:v1")

    mock_logger.warning.assert_called_once_with("Looking for pod with dirty image dirty-image-dirty:v1.")


@pytest.mark.parametrize(
    "user_input",
    ["N", "no", "nope", "nah", "", "   ", "q", "quit", "anything_else", "123", "maybe", "sure", "ok"],
)
def test_check_matching_image_dirty_image_user_refuses(user_input):
    """Should raise ValueError when user refuses or gives any non-yes input."""
    with patch("builtins.input", return_value=user_input):
        with pytest.raises(ValueError) as exc_info:
            _confirm_dirty_image("dirty-image-dirty:v1")

        assert "Thanks for refusing to connect to a pod with a dirty image." in str(exc_info.value)
