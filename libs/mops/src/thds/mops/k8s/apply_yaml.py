import subprocess
import tempfile

import kubernetes
from kubernetes import client, utils
from packaging import version


def format_yaml(yaml_template_str: str, **template_values: str) -> str:
    return yaml_template_str.format(**template_values)


def kubectl_apply_file(yaml_path: str) -> None:
    subprocess.run(["kubectl", "apply", "-f", yaml_path], check=True)


def kubectl_apply(yaml_string: str) -> None:
    with tempfile.NamedTemporaryFile("w", prefix="kubectl-yaml") as f:
        f.write(yaml_string)
        f.flush()
        kubectl_apply_file(f.name)


def apply_yaml(yaml_path: str) -> None:
    if version.parse(kubernetes.__version__) < version.parse("32.0.0"):
        kubectl_apply_file(yaml_path)  # best effort
        return

    # NOTE: Prior to 32.0.0, this function doesn't actually server-side apply.
    # https://github.com/kubernetes-client/python/pull/2252
    # Hence the check above to use kubectl for older versions.
    utils.create_from_yaml(client.ApiClient(), yaml_path, apply=True)


def create_yaml_template(yaml_str: str, **template_values: str) -> None:
    """Format a YAML template with the given keyword arguments, then apply it to the Kubernetes cluster.

    You must already have set up your SDK config.

    """
    formatted_yaml = format_yaml(yaml_str, **template_values)
    with tempfile.NamedTemporaryFile("w", prefix="kubectl-yaml") as f:
        f.write(formatted_yaml)
        f.flush()
        apply_yaml(f.name)
