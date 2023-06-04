from pathlib import Path

from thds.adls.global_client import get_global_client
from thds.mops.config import get_datasets_storage_root
from thds.mops.remote._adls import yield_filenames
from thds.mops.remote.adls_remote_files import AdlsRoot, _represent_adls_path


def sync_remote_to_local_as_pointers(
    directory: str, local_root: str = ".", sa: str = "", container: str = ""
):  # pragma: nocover
    """If your orchestrator process somehow dies but all the runners
    succeeded, you can 'recover' the results easily with this
    function, making it easy to move to the next step in your pipeline.

    Mostly intended for interactive use.

    e.g.:

    sync_remote_to_local_as_pointers(
        'demand-forecast/peter-gaultney-df-orch-2022-07-25T19:04:20-1188-train-Radiology/.cache',
        sa='thdsdatasets',
        container='ml-ops',
    )
    """
    local_root_path = Path(local_root)
    root = AdlsRoot.parse(get_datasets_storage_root())
    # normalize to start with no slash and end with a slash.
    directory = directory if directory.endswith("/") else (directory + "/")
    directory = directory[1:] if directory.startswith("/") else directory
    for azure_filename in yield_filenames(get_global_client(root.sa, root.container), directory):
        assert azure_filename.startswith(directory)
        path = local_root_path / azure_filename[len(directory) :]
        path.parent.mkdir(exist_ok=True, parents=True)
        print(path)
        with open(path, "w") as f:
            # TODO put b64(md5) in here as well; client.get_file_client(key).get_file_properties()...
            f.write(_represent_adls_path(sa, container, azure_filename))


if __name__ == "__main__":  # pragma: nocover
    import sys

    sync_remote_to_local_as_pointers(sys.argv[1])

# TODO write utility for replacing local remote file pointers with the actual files.
