# Kubernetes

We use Kubernetes for scalable general-purpose compute as well as
GPU-based compute. An example of GPU usage can be found in [mldemo](../../../apps/mldemo/README.md).

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

## Usage with mops `pure_remote`:

If using K8s to remotely run your `mops`-decorated
[pure function](./pure_functions.md), you will need to construct a
`k8s_shell` with configuration appropriate to the Python function that
will ultimately be invoked remotely. This includes specifying the name
of your Docker container as well as providing resource requests or
limitations. For example, you might create a module called
`k8s_shell.py`:

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

## Orchestrator pods

If you're running a big or long-running process with
[remote workers](./remote.md) in K8s, because of common network
[limitations](./limitations.md), you may wish to run from an
orchestrator pod also hosted in Kubernetes.

See [here](src/thds/mops/k8s/orchestrator/README.md).

## Further reading

See [the source README](../src/thds/mops/k8s/README.md) for more
specifics on things you can do with Kubernetes via `mops`.
