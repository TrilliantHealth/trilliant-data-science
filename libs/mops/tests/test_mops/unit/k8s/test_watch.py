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


def test_deleted_objects_keep_their_final_state_but_are_known_deleted():
    seen: watch._SeenObjectContainer[str, dict] = watch._SeenObjectContainer()
    seen.set_object("job-a", {"phase": "running"})
    assert not seen.was_deleted("job-a")

    seen.set_object("job-a", {"phase": "complete"}, deleted=True)
    assert seen.was_deleted("job-a")
    assert seen.get("job-a") == {"phase": "complete"}  # the final state still answers `get`

    seen.set_object("job-a", {"phase": "running"})  # re-created under the same name
    assert not seen.was_deleted("job-a")
    assert not seen.was_deleted("never-seen")
