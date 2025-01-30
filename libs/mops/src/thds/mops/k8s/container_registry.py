from . import config


def autocr(container_image_name: str, cr_url: str = "") -> str:
    """Prefix the container with the configured container registry URL.

    Idempotent, so it will not apply if called a second time.
    """
    cr_url = cr_url or config.k8s_acr_url()
    assert cr_url, "No container registry URL configured."
    prefix = cr_url + "/" if cr_url and not cr_url.endswith("/") else cr_url
    if not container_image_name.startswith(prefix):
        return prefix + container_image_name
    return container_image_name
