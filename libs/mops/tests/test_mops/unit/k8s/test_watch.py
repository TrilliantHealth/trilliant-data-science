import time

from thds.mops.k8s import config, watch


def test_is_stale_ignores_dead_api():
    now = time.monotonic()
    api_last_update_time = now - config.k8s_watch_object_stale_seconds() - 1
    assert not watch.is_stale(api_last_update_time, api_last_update_time - 1000000)


def test_not_stale_if_updated_recently():
    now = time.monotonic()
    api_last_update_time = now - 1
    assert not watch.is_stale(api_last_update_time, now - 17)


def test_stale_if_not_updated_recently():
    now = time.monotonic()
    api_last_update_time = now - 1
    assert watch.is_stale(api_last_update_time, now - config.k8s_watch_object_stale_seconds() - 1)
