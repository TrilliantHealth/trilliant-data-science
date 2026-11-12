import urllib3
from kubernetes import client

from thds.mops.k8s import auth

_ENV = {
    "HTTPS_PROXY": "http://proxy.example:3128",
    "NO_PROXY": "localhost,10.0.0.0/16,.svc",
}


def _configuration(host: str) -> client.Configuration:
    cfg = client.Configuration()
    cfg.host = host
    cfg.proxy = _ENV["HTTPS_PROXY"]
    return cfg


def test_api_server_in_no_proxy_is_reached_directly() -> None:
    cfg = auth._with_env_no_proxy(_configuration("https://10.0.0.1:443"), _ENV)
    pool = client.ApiClient(configuration=cfg).rest_client.pool_manager
    assert not isinstance(pool, urllib3.ProxyManager)


def test_hosts_outside_no_proxy_still_use_the_proxy() -> None:
    cfg = auth._with_env_no_proxy(_configuration("https://api.example.com"), _ENV)
    pool = client.ApiClient(configuration=cfg).rest_client.pool_manager
    assert isinstance(pool, urllib3.ProxyManager)


def test_lowercase_no_proxy_wins_and_absence_is_none() -> None:
    lower = auth._with_env_no_proxy(client.Configuration(), {**_ENV, "no_proxy": "only.this"})
    assert lower.no_proxy == "only.this"
    assert auth._with_env_no_proxy(client.Configuration(), {}).no_proxy is None
