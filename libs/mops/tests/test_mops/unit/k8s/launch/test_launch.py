import contextlib
import typing as ty
from unittest import mock

import pytest
from kubernetes import client

from thds.mops.k8s import _launch, cost, retry


@contextlib.contextmanager
def _mock_batch_api() -> ty.Iterator[ty.Tuple[mock.MagicMock, mock.MagicMock]]:
    with (
        mock.patch.object(_launch, "api_client"),
        mock.patch.object(_launch, "upsert_namespace") as upsert,
        mock.patch.object(cost, "ensure_started"),
        mock.patch.object(retry.time, "sleep"),
        mock.patch.object(client, "BatchV1Api") as api,
    ):
        yield api.return_value, upsert


def _launch_job() -> None:
    _launch.launch(
        "example.io/image:tag",
        ["run"],
        full_name="job-a",
        namespace="ns-b",
        kubeconfig_context="ctx-c",
        transform_job=lambda job: job,
    )


def test_job_name_is_the_same_on_every_create_attempt() -> None:
    with _mock_batch_api() as (api, _):
        api.create_namespaced_job.side_effect = [
            client.exceptions.ApiException(status=503),
            mock.DEFAULT,
        ]
        _launch_job()
    assert [c.kwargs["body"].metadata.name for c in api.create_namespaced_job.call_args_list] == [
        "job-a",
        "job-a",
    ]


def test_already_exists_after_a_retried_create_reads_the_existing_job() -> None:
    with _mock_batch_api() as (api, _):
        api.create_namespaced_job.side_effect = [
            client.exceptions.ApiException(status=504),
            client.exceptions.ApiException(status=409),
        ]
        _launch_job()
    api.read_namespaced_job.assert_called_once_with(name="job-a", namespace="ns-b")


def test_already_exists_on_the_first_create_raises() -> None:
    with _mock_batch_api() as (api, _):
        api.create_namespaced_job.side_effect = client.exceptions.ApiException(status=409)
        with pytest.raises(client.exceptions.ApiException):
            _launch_job()
    api.read_namespaced_job.assert_not_called()


def test_already_exists_after_a_retried_namespace_setup_raises() -> None:
    with _mock_batch_api() as (api, upsert):
        upsert.side_effect = [client.exceptions.ApiException(status=503), None]
        api.create_namespaced_job.side_effect = client.exceptions.ApiException(status=409)
        with pytest.raises(client.exceptions.ApiException):
            _launch_job()
    api.read_namespaced_job.assert_not_called()
