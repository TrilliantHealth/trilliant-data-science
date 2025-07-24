import typing as ty
from contextlib import contextmanager
from typing import ContextManager

import pytest

from thds.core import refcount


class AResource:
    def __init__(self, name: str):
        self.name = name
        self.created = True
        self.destroyed = False

    def cleanup(self):
        self.destroyed = True


@pytest.fixture
def resource_factory() -> tuple[
    ty.Callable[[], ContextManager[AResource]],
    list[AResource],
]:
    created_resources: list[AResource] = []

    @contextmanager
    def test_counting_resource_factory() -> ty.Iterator[AResource]:
        resource = AResource(f"resource_{len(created_resources)}")
        created_resources.append(resource)
        try:
            yield resource
        finally:
            resource.cleanup()

    return test_counting_resource_factory, created_resources


def test_basic_allocation_and_deallocation(resource_factory):
    factory, created_resources = resource_factory
    shared = refcount.Resource(factory)

    # Initially no resources created
    assert len(created_resources) == 0

    # First access creates resource
    with shared.get() as resource1:
        assert len(created_resources) == 1
        assert resource1.created
        assert not resource1.destroyed
        assert resource1.name == "resource_0"

    # After context exit, resource is cleaned up
    assert created_resources[0].destroyed

    # Second access creates new resource
    with shared.get() as resource2:
        assert len(created_resources) == 2
        assert resource2.created
        assert not resource2.destroyed
        assert resource2.name == "resource_1"

    # Second resource also cleaned up
    assert created_resources[1].destroyed


def test_nested_usage_same_thread(resource_factory):
    factory, created_resources = resource_factory
    shared = refcount.Resource(factory)

    with shared.get() as resource1:
        assert len(created_resources) == 1
        first_resource = created_resources[0]

        # Nested usage should reuse same resource
        with shared.get() as resource2:
            assert len(created_resources) == 1  # Still only one resource
            assert resource1 is resource2  # Same object
            assert not first_resource.destroyed

        # Still not destroyed after inner context
        assert not first_resource.destroyed

    # Only destroyed after all contexts exit
    assert first_resource.destroyed


def test_multiple_sequential_accesses(resource_factory):
    factory, created_resources = resource_factory
    shared = refcount.Resource(factory)

    for i in range(3):
        with shared.get() as resource:
            assert len(created_resources) == i + 1

    # All resources should be cleaned up
    assert len(created_resources) == 3
    for resource in created_resources:
        assert resource.destroyed

    # Each access got a different resource
    assert len(set(id(r) for r in created_resources)) == 3


def test_that_this_works_with_a_threadpoolexecutor_which_is_its_own_context_manager():
    from concurrent.futures import ThreadPoolExecutor

    # Create a Resource that manages a ThreadPoolExecutor
    shared_executor = refcount.Resource(lambda: ThreadPoolExecutor(max_workers=2))

    with shared_executor.get() as executor:

        def task():
            return "task completed"

        with executor as exec_:
            # Submit a task to the executor
            future = exec_.submit(task)
            result = future.result()
            assert result == "task completed"

        # after this context exit, the threadpool should still be usable
        with shared_executor.get() as exec_:
            # Submit another task to the same executor
            future = exec_.submit(task)
            result = future.result()
            assert result == "task completed"
