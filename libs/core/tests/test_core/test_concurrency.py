from thds.core import concurrency, env


def test_contextful_concurrency():
    with concurrency.contextful_threadpool_executor() as executor:
        future = executor.submit(env.active_env)
        assert future.result() == "dev"

    with env.temp_env("prod"):
        with concurrency.contextful_threadpool_executor() as executor:
            future = executor.submit(env.active_env)
            assert future.result() == "prod"
