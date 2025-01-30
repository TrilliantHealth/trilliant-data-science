# Run Jobs on DS Kubernetes cluster

Launch a K8s Job on an existing Docker image, using Python for configuration.

Optionally (but by default):

1. Scrape logs of the launched pod(s) reliably.
1. Wait for the Job to exit.

> NOT intended for deployment of a long-running service! For that, use Helm.

## Launch

> This is the low level, imperative API. It is most suitable for launching a K8s image where the program
> being run is not Python.
>
> If you are running Python, you should look into using the declarative approach supported by
> `mops.pure.PickleRunner`.

```python
from thds.mops.k8s import launch, autocr

launch(
    autocr('ds/demand-forecast'),
    # autocr uses thds.mops configuration to select the correct Azure Container Registry
    ['python', '-m', 'demandforecast.main', ...],
    name_prefix='df-train',
)

# `launch` returns None on success; raises K8sJobFailedError if the
# Job enters a Failed state.  Does not suppress exceptions
# encountered during Job creation or while waiting for the Job to finish.
```

### Node narrowing

You probably want to tell Kubernetes how many resources you need. That should look something like this:

```python
from thds.mops.k8s import launch, autocr, tolerates_spot

launch(
    autocr('foo/bar'),
    ['some', 'args'],
    node_narrowing=dict(
        resource_requests=dict(cpu='3.0', memory: '8.8G'),
        tolerations=[tolerates_spot()], # obviously don't do this if you don't want spot instances...
    ),
)
```

`resource_limits` are also available. Ask your local `mops` dealership for details.

## Log scraping

Printed in a randomly-selected CSS color for each pod. If your shell/terminal don't support these colors,
it'll probably be weird.

If you do not wish to scrape logs for some reason, set the `MOPS_NO_K8S_LOGS` environment variable, or
pass `suppress_logs=True` to `launch()`.
