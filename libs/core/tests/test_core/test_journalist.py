import logging
import threading
import time

from thds.core import journalist


def _sample(wall, cpu, rss_mb=100.0, cgroup_mb=None, net=None, disk=None):
    return journalist._Sample(
        wall=wall,
        cpu_seconds=cpu,
        rss_mb=rss_mb,
        cgroup_mb=cgroup_mb,
        net=net,
        disk=disk,
    )


def _fresh_journalist(label, **kw):
    kw.setdefault("interval", 100.0)
    kw.setdefault("sample_interval", 100.0)
    return journalist.Journalist(label, **kw)


def test_single_entry_registers_and_deregisters():
    j = _fresh_journalist("solo")
    if not j._enabled:  # psutil missing - feature not applicable
        return

    assert "solo" not in journalist._SAMPLER.active_labels()
    with j as active:
        assert "solo" in journalist._SAMPLER.active_labels()
        assert active._enabled
    assert "solo" not in journalist._SAMPLER.active_labels()


def test_nested_journalists_both_active():
    outer = _fresh_journalist("outer")
    inner = _fresh_journalist("inner")
    if not outer._enabled:
        return

    with outer:
        assert journalist._SAMPLER.active_labels() == {"outer"}
        with inner:
            assert journalist._SAMPLER.active_labels() == {"outer", "inner"}
        assert journalist._SAMPLER.active_labels() == {"outer"}
    assert journalist._SAMPLER.active_labels() == set()


def test_fold_accumulates_peaks_and_avg():
    # fold() operates on synthetic _Samples and is independent of psutil, so this
    # runs meaningfully even where psutil (and thus the live sampler) is absent.
    j = _fresh_journalist("fold")
    # The first fold is a baseline only (seeds the deltas, not counted as a
    # sample) - matching the original loop, which read once before counting.
    j.fold(_sample(wall=0.0, cpu=0.0, rss_mb=100.0))
    j.fold(_sample(wall=1.0, cpu=1.0, rss_mb=300.0))
    j.fold(_sample(wall=2.0, cpu=2.0, rss_mb=200.0))

    assert j.peak_rss_mb == 300.0
    assert j.avg_rss_mb == 250.0  # (300 + 200) / 2; baseline sample not counted
    # 1 cpu-second per 1 wall-second == 1 core, sustained.
    assert abs(j.peak_cpu_cores - 1.0) < 0.01
    # avg over the whole window: 2 cpu-seconds across 2 wall-seconds == 1 core.
    assert abs(j.avg_cpu_cores - 1.0) < 0.01


def test_nested_totals_are_a_subset_of_outer():
    outer = _fresh_journalist("outer")
    inner = _fresh_journalist("inner")
    # net = (sent, recv) cumulative bytes. Feed the same stream to both, but the
    # inner journalist only sees the middle of the run.
    outer.fold(_sample(wall=0.0, cpu=0.0, net=(0, 0)))
    outer.fold(_sample(wall=1.0, cpu=0.0, net=(0, 1_000_000_000)))
    inner.fold(_sample(wall=1.0, cpu=0.0, net=(0, 1_000_000_000)))
    outer.fold(_sample(wall=2.0, cpu=0.0, net=(0, 2_000_000_000)))
    inner.fold(_sample(wall=2.0, cpu=0.0, net=(0, 2_000_000_000)))
    # inner exits here; outer keeps going.
    outer.fold(_sample(wall=3.0, cpu=0.0, net=(0, 5_000_000_000)))

    assert outer.total_recv_gb > inner.total_recv_gb
    # inner saw 1 GB -> 2 GB == 1 GB; outer saw 0 -> 5 GB == 5 GB.
    assert abs(inner.total_recv_gb - (1_000_000_000 / 1024**3)) < 0.001
    assert abs(outer.total_recv_gb - (5_000_000_000 / 1024**3)) < 0.001


def test_concurrent_label_collision_is_suffixed(caplog):
    a = _fresh_journalist("query")
    b = _fresh_journalist("query")
    if not a._enabled:
        return

    with caplog.at_level(logging.WARNING, logger="thds.core.journalist"):
        with a:
            with b:
                assert journalist._SAMPLER.active_labels() == {"query", "query#2"}
    assert any("query" in r.getMessage() for r in caplog.records)


def test_sequential_reuse_is_not_suffixed():
    if not _fresh_journalist("x")._enabled:
        return

    with _fresh_journalist("query"):
        pass
    with _fresh_journalist("query"):
        assert journalist._SAMPLER.active_labels() == {"query"}


def test_single_sampler_thread_across_nested_journalists():
    outer = _fresh_journalist("outer", sample_interval=0.02)
    inner = _fresh_journalist("inner", sample_interval=0.02)
    if not outer._enabled:
        return

    def _sampler_threads():
        return [t for t in threading.enumerate() if t.name == journalist._SAMPLER_THREAD_NAME]

    assert _sampler_threads() == []
    with outer:
        with inner:
            time.sleep(0.1)
            assert len(_sampler_threads()) == 1
    # give the thread a moment to wind down after the last exit.
    time.sleep(0.1)
    assert _sampler_threads() == []


def test_fold_after_exit_is_a_no_op():
    # A fold from a stale sampler snapshot can arrive after a journalist exits.
    # It must not mutate metrics or emit a line - otherwise a periodic line prints
    # after the final summary and metrics change post-exit. The `_active` flag,
    # cleared in __exit__, makes fold self-veto without any per-fold locking.
    j = _fresh_journalist("late")
    if not j._enabled:
        return

    j.fold(_sample(wall=0.0, cpu=0.0, rss_mb=100.0))  # baseline
    j.fold(_sample(wall=1.0, cpu=1.0, rss_mb=200.0))  # one real sample
    j.__exit__()  # clears _active (no register/deregister needed to test the guard)

    before = j.metrics
    j.fold(_sample(wall=2.0, cpu=5.0, rss_mb=999.0))  # would spike peak/cpu if it ran
    assert j.metrics == before
    assert j.peak_rss_mb == 200.0  # not 999


def test_late_fold_emits_no_log_line(caplog):
    # The observable symptom the flag prevents: a periodic log line after exit.
    j = _fresh_journalist("late-log", interval=0.0)  # interval 0 => every fold logs
    if not j._enabled:
        return

    j.fold(_sample(wall=0.0, cpu=0.0))
    j.__exit__()

    with caplog.at_level(logging.INFO, logger="thds.core.journalist"):
        j.fold(_sample(wall=1.0, cpu=1.0))
    assert [r for r in caplog.records if "late-log" in r.getMessage()] == []


def test_finest_sample_interval_wins():
    outer = _fresh_journalist("outer", sample_interval=100.0)
    inner = _fresh_journalist("inner", sample_interval=0.5)
    if not outer._enabled:
        return

    with outer:
        assert journalist._SAMPLER.current_interval() == 100.0
        with inner:
            assert journalist._SAMPLER.current_interval() == 0.5
        assert journalist._SAMPLER.current_interval() == 100.0


def test_exit_logs_final_summary(caplog):
    # Large `interval` so the sample loop's periodic log never fires inside the
    # with-block. The only log emitted should be the final-summary on __exit__.
    j = _fresh_journalist("summary-test", interval=100.0, sample_interval=0.05)
    if not j._enabled:
        return

    with caplog.at_level(logging.INFO, logger="thds.core.journalist"):
        with j:
            time.sleep(0.3)

    lines = [r.getMessage() for r in caplog.records if "summary-test" in r.getMessage()]
    assert len(lines) == 1
    assert "MEM" in lines[0]
    assert "CPU" in lines[0]


def test_metrics_construct_with_no_args():
    # Adding a metric field must stay additive: downstream code that builds a
    # baseline/zero instance shouldn't have to enumerate every field.
    metrics = journalist.JournalistMetrics()
    assert metrics.peak_disk_read_mbps == 0.0
    assert metrics.total_disk_write_gb == 0.0
    assert metrics.elapsed_seconds is None
