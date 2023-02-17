# `mops`

`mops` is a tool for running large workloads in parallel on an
existing Kubernetes cluster using pure Python.

Prerequisites:
 * Python 3.7 or newer
 * You can login to Azure Cloud with `az login`
 * Your Azure Active Directory credentials include access to Azure Kubernetes Service (AKS)

## Installation

`pip install thds.mops` (or better yet, add `thds.mops` to your
`pyproject.toml`).

### Required Kubernetes Setup

This is now documented in the [Guidebook](https://guidebook.trillianthealth.com/data-science/kubernetes-access/).

### Additional Cluster setup [optional but recommended as of Q1 2023]

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

## Running Your Code on K8s

See [the nested readme here](src/thds/mops/k8s/README.md) for basics on how
to launch a known Docker image in Kubernetes directly, and optionally
wait for it to complete.

### Call Python function in parallel on Kubernetes

Alternatively, instead of manually managing jobs, you can just call
Python functions that have a Docker image backing them!

Take a look at our sub-READMEs on the `pure_remote`
[function decorator](src/thds/mops/remote/README.md).

Used together, these utilities will allow you to parallelize parts
(pickleable functions) of your Python application across the cluster,
similar to how `multiprocessing.Pool` and friends work.

### Orchestrator pods

See [here](src/thds/mops/k8s/orchestrator/README.md).

# Development

If making changes to the library, please bump the version in `pyproject.toml` accordingly.
