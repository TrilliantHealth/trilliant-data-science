"""Where a mops remote is running, in Kubernetes terms.

Lives here rather than in the console because these are the same variables `_launch` sets
on the container: the writer and the reader of a name belong together, and `mops` core has
no business enumerating the runtimes it might be launched by.

Named by `MOPS_RUNTIME_CONTEXT` in the remote's environment, which `_launch` sets to this
function by default.
"""

import os

from thds.mops.pure.tools.console import runtime

from . import config


def k8s_context() -> runtime.RuntimeContext:
    """A pod is addressable while it lives and gone minutes after it exits, so this is read
    on the remote, at the moment it starts, rather than reconstructed later.

    All three coordinates are needed to reach the logs; a pod name alone is not enough.
    """
    return runtime.RuntimeContext(
        "k8s",
        dict(
            pod_name=os.getenv("HOSTNAME", ""),
            namespace=os.getenv(config.k8s_namespace_env_var_key(), ""),
            job_name=os.getenv("MOPS_K8S_JOB_NAME", ""),
        ),
    )
    # the namespace variable is named by config, because `_launch` lets a deployment choose
    # what to call it - reading a hardcoded name here would work everywhere except the
    # deployments that changed it.


PROVIDER = f"{k8s_context.__module__}.{k8s_context.__name__}"
