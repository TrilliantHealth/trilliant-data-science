# Kubernetes

We use Kubernetes for scalable general-purpose compute as well as
GPU-based compute. An example of GPU usage can be found in [mldemo](../../../apps/mldemo/README.md).

This library allows imperative and declarative launching of K8s Jobs
on specified Docker images, and will by default scrape the logs of the
pods run by the Jobs.

This is _not_ intended to replace the use of Helm for long-running
services (e.g. APIs).

## Required Kubernetes Setup

This is now documented in the [Guidebook](https://guidebook.trillianthealth.com/data-science/kubernetes-access/).

### Additional Cluster setup [optional but recommended as of Q2 2023]

These steps are not required, and will also not benefit anyone who is
not using the `mops` library as part of their application (e.g. who is
just deploying applications via Helm).

Background: A K8s namespace using a sanitized version your local
computer's user account name will eventually be created if you use
this library. If that generated namespace does not appear in the list
of `common_azure_access_identity.namespaces`
[here](https://github.com/TrilliantHealth/engineering-infra/blob/main/engineering-stable/datascience/identities.tf#L4),
and the corresponding default config value for
`namespaces_supporting_workload_identity`
[in this library](src/thds/mops/east_config.toml), then you are more
likely to run into non-deterministic Azure authentication failures
because of the
[older](https://learn.microsoft.com/en-us/azure/aks/use-azure-ad-pod-identity)
authn/authz method that must be used to support your unknown
namespace.

It's recommended that you make a small PR to `engineering-infra` and a
corresponding PR to this library to add your namespace. This will
automatically enable the use of
[more reliable and performant auth](https://azure.github.io/azure-workload-identity/docs/introduction.html)
from your K8s pods against Azure resources such as ADLS.

Unfortunately this manual step appears to be unavoidable at this time,
because the configuration must live in a part of Azure that we do not
have permission to directly edit.

## Declarative usage with mops `pure_remote`:

This approach is preferred for cases where the code to be run on
Kubernetes can be considered a [pure function](./pure_functions.md)
and is implemented as or wrapped by Python.

> If you're trying to wrap a Docker image that you don't control,
> you'll need to use the low-level imperative API instead.

You will need to construct a `k8s_shell` with configuration
appropriate to the Python function that will ultimately be invoked
remotely. This includes specifying the name of your Docker container
as well as providing resource requests or limitations. For example,
you might create a module called `k8s_shell.py`:

```python
import thds.mops.k8s as mops_k8s

df_k8s_spot_11g_shell = mops_k8s.k8s_shell(
    mops_k8s.autocr('ds/demand-forecast:latest'),
    # gotta specify which container this runs on
    node_narrowing=dict(
        resource_requests={"cpu": "3.6", "memory": "11G"},
        tolerations=[mops_k8s.tolerates_spot()],
    ),
)
```

> `mops` does _not_ distribute your code for you, and neither does the
> provided k8s shell. You must perform your own code distribution by
> building and pushing a Docker image to an accessible container
> registry, and configuring the K8s Shell so that it knows what
> pre-published image to use.

This will require your Docker image to exist and to be accessible to
Kubernetes via the configured Container Registry. Additionally, that
image must somehow be provided with read and write access to ADLS,
since all data will be transferred back and forth from your local
environment via ADLS. All Data Science K8s clusters will provide
read/write access to a set of ADLS Storage Accounts by default.

Once you have contructed an appropriate Shell, you need only pass it
to the `AdlsPickleRunner` constructor, and then use `pure_remote` plus
that runner to construct a decorator that will transmit computation
for any pure function to the remote K8s environment, and inject its
result right back into the local/orchestrator context.

## Imperative usage

Declarative usage is recommended because it confers additional
capabilities such as memoization (and therefore a form of
fault-tolerance) but if you have a non-Python image you want to
launch, see the docs on the low-level imperative interface
[here](../src/thds/mops/k8s/README.md).

The basic container specification will be the same regardless of which
approach is used.

## Logging

For Kubernetes only, `mops` will automatically try to scrape and print
the logs from each launched pod. ANSI terminal Colors will be
automatically chosen for each separate pod being scraped.

If you are running many pods in parallel, it may be desirable to turn
off logging, as it has been observed to leak memory over the course of
long runs, and a separate OS thread is used for each pod being
scraped.

Either set the `MOPS_NO_K8S_LOGS` environment variable, or
programmatically pass `suppress_logs=True` to either `k8s_shell`
(declarative API) or `launch` (imperative API).

## Common failure modes

> This list is not even close to exhaustive, but we can add to it
> over time.

### Too little memory.

Kubernetes will often
[kill pods that use more than their requested memory](https://cloud.google.com/blog/products/containers-kubernetes/kubernetes-best-practices-resource-requests-and-limits),
but what makes this sometimes tricky to diagnose is that Kubernetes
will _not_ kill pods _because_ they used more memory than requested -
pod death will only occur when _other_ pods scheduled on the same node
also end up using more memory than they requested... and then the node
itself runs out of RAM, and K8s has to choose a pod to kill.

If you are CPU bound, you may as well request roughly 4x more RAM than
CPU, since CPU-optimized nodes generally have 4GB of RAM for each
CPU. If you are strictly memory bound, you will simply need to request
enough memory for the worst case scenario for each of your Jobs.

If you have a hybrid workload (different types of Jobs running
concurrently) and are willing to occasionally let partially-complete
pods die from OOM, you can set your memory requests lower than the max
pressure – may the odds be ever in your favor ;)

### Missing Docker image.

We're working to make this easier to identify when using `mops`, but
on a basic level, the Kubernetes API does not ever reject a request to
launch a Job or Pod when provided with a Docker image tag that it
can't find. It will happily accept your request, and then there will
just be infinite ImageBackOff errors that can be found by querying the
API (or using a console like k9s). This is fundamentally because K8s
wants to make allowance for the possibility that your container
registry might go down for periods of time, which shouldn't prevent
you from making a request for something to work once the registry
comes back up.

In any case, if you're running a new process, or have recently changed
something about your Docker image, keep an eye on the API or console
for ImageBackOff errors that might be an indication that you've asked
`mops` to create a Job with an image that simply does not exist.

Alternatively, you can take a look at the
[machinery](../src/thds/mops/k8s/image_backoff) we have
[put in place](https://github.com/TrilliantHealth/demand-forecast/blob/main/demandforecast/k8s_choose_image.py#L18)
that may help you detect this.

## Orchestrator pods

If you're running a big or long-running process with
[remote workers](./remote.md) in K8s, because of common network
[limitations](./limitations.md), you may wish to run from an
orchestrator pod also hosted in Kubernetes.

See [here](../src/thds/mops/k8s/orchestrator/README.md).

## Further reading

See [the source README](../src/thds/mops/k8s/README.md) for more
specifics on things you can do with Kubernetes via `mops`.
