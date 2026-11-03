import typing as ty
from unittest import mock

import pytest
from kubernetes import client

from thds.mops.k8s import config, retry


def _flaky(*errors: Exception) -> mock.MagicMock:
    return mock.MagicMock(side_effect=[*errors, "ok"])


@pytest.fixture(autouse=True)
def sleeps() -> ty.Iterator[mock.MagicMock]:
    with mock.patch.object(retry.time, "sleep") as sleep:
        yield sleep


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retries_transient_api_status(status: int) -> None:
    f = _flaky(client.exceptions.ApiException(status=status))
    assert retry.k8s_sdk_retry()(f)() == "ok"
    assert f.call_count == 2


def test_transient_api_errors_back_off_exponentially(sleeps: mock.MagicMock) -> None:
    f = _flaky(*[client.exceptions.ApiException(status=503)] * 3)
    retry.k8s_sdk_retry()(f)()
    assert [c.args[0] for c in sleeps.call_args_list] == [1, 2, 4]


def test_transient_api_errors_give_up_after_five_retries() -> None:
    f = mock.MagicMock(side_effect=client.exceptions.ApiException(status=503))
    with pytest.raises(client.exceptions.ApiException):
        retry.k8s_sdk_retry()(f)()
    assert f.call_count == 6


def test_transient_api_retry_limit_is_configurable() -> None:
    f = mock.MagicMock(side_effect=client.exceptions.ApiException(status=503))
    with config.k8s_transient_api_error_max_retries.set_local(1):
        with pytest.raises(client.exceptions.ApiException):
            retry.k8s_sdk_retry()(f)()
    assert f.call_count == 2


@pytest.mark.parametrize("status", [400, 403, 404, 409, 422])
def test_other_api_statuses_raise_at_once(status: int) -> None:
    f = mock.MagicMock(side_effect=client.exceptions.ApiException(status=status))
    with pytest.raises(client.exceptions.ApiException):
        retry.k8s_sdk_retry()(f)()
    assert f.call_count == 1
