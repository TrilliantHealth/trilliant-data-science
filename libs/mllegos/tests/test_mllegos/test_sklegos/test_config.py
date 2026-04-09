import os

import joblib

from thds.mllegos.sklegos import config


def test_local_config_restores_env():
    env = dict(os.environ)
    with config.configure_local(num_cores=1000):  # absurd number
        assert dict(os.environ) != env
    assert dict(os.environ) == env


def test_local_config_restores_joblib_config():
    conf = joblib.parallel_config().parallel_config.copy()
    with config.configure_local(num_cores=1000, backend="threading") as new_conf:  # absurd number
        assert new_conf.old_parallel_config == conf
        assert new_conf.parallel_config != conf
        assert "threading" in type(new_conf.parallel_config["backend"]).__name__.lower()
        assert new_conf.parallel_config["n_jobs"] == 1000
    assert joblib.parallel_config().parallel_config == conf
