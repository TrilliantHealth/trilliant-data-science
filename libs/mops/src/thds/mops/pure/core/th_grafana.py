"""TH-internal Grafana log URL generator for mops result metadata.

This module is excluded from OSS releases.
"""

import json
import os
import typing as ty
from urllib.parse import quote

from .metadata import ResultMetadata

GRAFANA_DATASOURCE_UID = "P8E80F9AEF21F6940"


def _build_grafana_url(logql_query: str, from_ms: int, to_ms: int) -> str:
    """Build a Grafana explore URL with the given LogQL query and time range."""
    panes_data = {
        "vgc": {
            "datasource": GRAFANA_DATASOURCE_UID,
            "queries": [{"refId": "A", "expr": logql_query, "queryType": "range"}],
            "range": {"from": str(from_ms), "to": str(to_ms)},
        }
    }
    panes_json = json.dumps(panes_data, separators=(",", ":"))

    return (
        f"https://grafana.devtools.trillianthealth.com/explore"
        f"?schemaVersion=1&panes={quote(panes_json, safe='')}&orgId=1"
    )


def grafana_log_url_generator(result_metadata: ResultMetadata) -> ty.Dict[str, str]:
    """Generate Grafana log URL metadata for mops results.

    This generator adds a clickable Grafana link to mops result metadata files,
    making it easy to jump directly to relevant logs when debugging pipeline runs.

    Only generates metadata when running in a k8s environment (detected via env vars).
    Returns empty dict for non-k8s runs, allowing graceful degradation.
    """
    job_name = os.environ.get("MOPS_K8S_JOB_NAME", "")
    namespace = os.environ.get("MOPS_K8S_NAMESPACE", "")
    pod_name = os.environ.get("HOSTNAME", "")
    image_ref = os.environ.get("MOPS_IMAGE_RECURSIVE_REF", "")
    cpus_guarantee = os.environ.get("THDS_CORE_CPUS_GUARANTEE", "")
    cpus_limit = os.environ.get("THDS_CORE_CPUS_LIMIT", "")
    memory_guarantee = os.environ.get("MOPS_K8S_MEMORY_GUARANTEE", "")
    memory_limit = os.environ.get("MOPS_K8S_MEMORY_LIMIT", "")

    if not (namespace and (pod_name or job_name)):
        return {}

    # Build LogQL query: stream selector for namespace, then label filter for pod.
    # Prefer exact pod name (each run's logs isolated), fall back to job name regex
    # (catches all pods from the job, including restarts).
    if pod_name:
        logql_query = f'{{namespace="{namespace}"}} | pod = `{pod_name}`'
    else:
        logql_query = f'{{namespace="{namespace}"}} | pod =~ `{job_name}.*`'

    # Convert timestamps to milliseconds for Grafana URL
    # Add buffer: 5 minutes before start, 5 minutes after end
    from_ms = int((result_metadata.remote_started_at.timestamp() - 300) * 1000)
    to_ms = int((result_metadata.remote_ended_at.timestamp() + 300) * 1000)

    result: ty.Dict[str, str] = {
        "grafana_logs": _build_grafana_url(logql_query, from_ms, to_ms),
    }

    if pod_name:
        result["k8s_pod_name"] = pod_name
    result["k8s_job_name"] = job_name
    result["k8s_namespace"] = namespace
    if image_ref:
        result["k8s_image"] = image_ref
    if cpus_guarantee:
        result["k8s_cpus_guarantee"] = cpus_guarantee
    if cpus_limit:
        result["k8s_cpus_limit"] = cpus_limit
    if memory_guarantee:
        result["k8s_memory_guarantee"] = memory_guarantee
    if memory_limit:
        result["k8s_memory_limit"] = memory_limit

    return result
