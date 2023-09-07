from pathlib import Path

from thds.adls import AdlsFqn, resource
from thds.adls.global_client import get_global_client
from thds.mops.pure.adls._files import yield_filenames
from thds.mops.srcdest.destf_pointers import _write_serialized_to_dest_placeholder


def sync_remote_to_local_as_pointers(uri: str, local_root: str = "."):  # pragma: nocover
    """If your orchestrator process somehow dies but all the runners
    succeeded, you can 'recover' the results easily with this
    function, making it easy to move to the next step in your pipeline.

    Mostly intended for interactive use.

    e.g.:

    sync_remote_to_local_as_pointers(
        'demand-forecast/peter-gaultney-df-orch-2022-07-25T19:04:20-1188-train-Radiology/',
        '.cache',
    )
    """
    local_root_path = Path(local_root)
    fqn = AdlsFqn.parse(uri)
    directory = fqn.path
    # normalize to start with no slash and end with a slash.
    directory = directory if directory.endswith("/") else (directory + "/")
    directory = directory[1:] if directory.startswith("/") else directory
    for azure_filename in yield_filenames(get_global_client(fqn.sa, fqn.container), directory):
        assert azure_filename.startswith(directory)
        path = local_root_path / azure_filename[len(directory) :]
        path.parent.mkdir(exist_ok=True, parents=True)
        print(path)
        # TODO put b64(md5) in here as well; client.get_file_client(key).get_file_properties()...
        _write_serialized_to_dest_placeholder(
            str(path), str(resource.AHR(AdlsFqn(fqn.sa, fqn.container, azure_filename), ""))
        )


if __name__ == "__main__":  # pragma: nocover
    import sys

    sync_remote_to_local_as_pointers(sys.argv[1])

# TODO write utility for replacing local remote file pointers with the actual files.
