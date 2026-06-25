"""Tests for the pod-log watcher's watch/follow kwarg detection.

`watch.Watch.get_watch_argument_name` scans an API method's docstring for the literal
`:param bool follow:`. kubernetes-client 36.x reformatted that to `:param follow:` +
`:type follow: bool`, so the stock heuristic returned `watch` (which
`read_namespaced_pod_log` rejects). `_PodLogWatch` adds the modern markers; these tests
use synthetic docstrings so they pin the behavior independent of the installed client.
"""

from thds.mops.k8s.logging import _PodLogWatch


def _func_with_doc(doc: str):
    def f():  # pragma: no cover - never called; only its __doc__ is read
        pass

    f.__doc__ = doc
    return f


def test_modern_docstring_format_picks_follow() -> None:
    # kubernetes-client 36.x style.
    f = _func_with_doc(":param follow: Follow the log stream.\n:type follow: bool")
    assert _PodLogWatch().get_watch_argument_name(f) == "follow"


def test_legacy_docstring_format_picks_follow() -> None:
    # kubernetes-client <=35.x style, handled by the base class via super().
    f = _func_with_doc(":param bool follow: Follow the log stream. Defaults to false.")
    assert _PodLogWatch().get_watch_argument_name(f) == "follow"


def test_non_follow_method_picks_watch() -> None:
    f = _func_with_doc(":param bool watch: Watch for changes.")
    assert _PodLogWatch().get_watch_argument_name(f) == "watch"
