import typing as ty

import azure.core.exceptions
from azure.storage.filedatalake import FileSystemClient


def yield_files(fsc: FileSystemClient, adls_root: str) -> ty.Iterable[ty.Any]:
    """Yield files (including directories) from the root."""
    with fsc as client:
        try:
            yield from client.get_paths(adls_root)
        except azure.core.exceptions.ResourceNotFoundError as rnfe:
            if rnfe.response and rnfe.response.status_code == 404:
                return  # no paths
            raise


def yield_filenames(fsc: FileSystemClient, adls_root: str) -> ty.Iterable[str]:
    """Yield only real file (not directory) names recursively from the root."""
    for azure_file in yield_files(fsc, adls_root):
        if not azure_file.get("is_directory"):
            yield azure_file["name"]
