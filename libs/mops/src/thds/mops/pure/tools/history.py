"""Find out how long a run took by looking at outputs to ADLS."""

import typing as ty
from datetime import timezone

from thds.adls.global_client import get_global_fs_client

from ..adls._files import yield_files


def summarize(sa: str, container: str, pipeline_root_dir: str) -> ty.Dict[str, ty.Any]:
    times = list()
    durations = list()
    total_functions = 0
    for azure_file in yield_files(get_global_fs_client(sa, container), pipeline_root_dir):
        if azure_file.name.endswith("invocation"):
            total_functions += 1
        times.append(azure_file.creation_time)
        last_modified = azure_file.last_modified.replace(tzinfo=timezone.utc)
        durations.append(last_modified - azure_file.creation_time)

    durations = sorted(durations)
    times = sorted(times)

    start = times[0]
    end = times[-1]

    max_duration = durations[-1]
    return dict(
        start=start,
        end=end,
        duration=end - start,
        slowest_file_upload=max_duration,
        total_functions=total_functions,
    )
