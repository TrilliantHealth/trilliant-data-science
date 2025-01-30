import tempfile

from kubernetes import client, utils


def format_yaml(yaml_template_str: str, **template_values: str) -> str:
    return yaml_template_str.format(**template_values)


def create_yaml_template(yaml_str: str, **template_values: str) -> None:
    """Format a YAML template with the given keyword arguments, then apply it to the Kubernetes cluster.

    You must already have set up your SDK config.

    NOTE: This function doesn't actually apply, and can't until the next release of the K8S SDK:
    https://github.com/kubernetes-client/python/pull/2252
    """
    formatted_yaml = format_yaml(yaml_str, **template_values)
    with tempfile.NamedTemporaryFile("w", prefix="kubectl-yaml") as f:
        f.write(formatted_yaml)
        f.flush()
        utils.create_from_yaml(client.ApiClient(), f.name)
