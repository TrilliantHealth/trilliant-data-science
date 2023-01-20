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

This _should_ be one-time setup, since we're down to a single cluster.

```bash
brew install kubectl Azure/kubelogin/kubelogin

az login  # since the East migration, you likely only have one choice here. If you have multiple, choose the one that gives you access to the East2 region.
az aks get-credentials --resource-group datascience --name datascience --subscription engineering-stable
kubelogin convert-kubeconfig
kubectl get nodes  # this will likely prompt you to open a web page to authenticate.
```

You will be prompted to open your browser, possibly multiple times;
If all goes well you'll have a `~/.kube/config`
file filled in nicely, and you won't have to run this command again.

## Running Your Code on K8s

See [the nested readme here](src/thds/k8s/README.md) for basics on how
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

# Development

If making changes to the library, please bump the version in `pyproject.toml` accordingly.
