"""The in-flight registry must never pin an invocation holder: entries are weakly held
and evaporate with their holder."""

import concurrent.futures
import gc

from thds.mops.pure.runner import same_process_in_flight


def test_register_peek_and_overwrite():
    first: concurrent.futures.Future = concurrent.futures.Future()
    second: concurrent.futures.Future = concurrent.futures.Future()

    same_process_in_flight.register("memo://unit/in-flight", first)
    assert same_process_in_flight.peek("memo://unit/in-flight") is first

    same_process_in_flight.register("memo://unit/in-flight", second)  # placeholder -> real future
    assert same_process_in_flight.peek("memo://unit/in-flight") is second

    assert same_process_in_flight.peek("memo://unit/never-registered") is None


def test_entry_evaporates_when_holder_is_garbage_collected():
    holder: concurrent.futures.Future = concurrent.futures.Future()
    same_process_in_flight.register("memo://unit/gc", holder)
    assert same_process_in_flight.peek("memo://unit/gc") is holder

    del holder
    gc.collect()
    assert same_process_in_flight.peek("memo://unit/gc") is None
