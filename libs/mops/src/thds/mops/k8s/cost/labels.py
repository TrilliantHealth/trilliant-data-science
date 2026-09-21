"""Kubernetes identity for Jobs included in one console cost ledger."""

import copy
import hashlib
import os

from kubernetes import client

RUN_LABEL = "mops.trillianthealth.com/console-run"
RUN_ANNOTATION = "mops.trillianthealth.com/console-run-name"


def value(console_run: str) -> str:
    """A stable Kubernetes-label-safe identity for a console run."""
    return hashlib.sha256(console_run.encode()).hexdigest()[:32]


def with_run(job: client.V1Job, console_run: str) -> client.V1Job:
    """Return a Job whose pods are selectable by the run's observer."""
    if not console_run:
        return job

    labelled = copy.deepcopy(job)
    metadata = labelled.spec.template.metadata
    metadata.labels = {**(metadata.labels or {}), RUN_LABEL: value(console_run)}
    metadata.annotations = {
        **(metadata.annotations or {}),
        RUN_ANNOTATION: console_run,
    }
    return labelled


def add_to(job: client.V1Job) -> client.V1Job:
    """Add the current process's console run identity to a Job copy."""
    return with_run(job, os.getenv("THDS_MOPS_CONSOLE_RUN_NAME", ""))
