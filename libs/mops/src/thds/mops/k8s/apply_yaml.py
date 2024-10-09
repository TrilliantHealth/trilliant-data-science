import tempfile

from kubernetes import client, utils


def apply_yaml_template(yaml_str: str, **kwargs: str):
    """Format a YAML template with the given keyword arguments, then apply it to the Kubernetes cluster.

    You must already have set up your SDK config.
    """
    formatted_yaml = yaml_str.format(**kwargs)
    with tempfile.NamedTemporaryFile("w", prefix="kubectl-yaml") as f:
        f.write(formatted_yaml)
        f.flush()
        utils.create_from_yaml(client.ApiClient(), f.name)
