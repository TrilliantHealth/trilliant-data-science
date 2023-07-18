from unittest import mock

from thds.core.env import active_env, set_active_env


@mock.patch("os.environ", {"THDS_ENV": "prod"})
def test_prod_env_via_envvar():
    assert active_env() == "prod"


def test_default_env():
    assert active_env() == "dev"
    assert active_env("foobar") == "foobar"  # type: ignore


def test_set_active_env_then_get():
    set_active_env("prod")
    assert active_env() == "prod"
    set_active_env("")
    assert active_env() == "dev"
