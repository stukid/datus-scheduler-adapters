# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from datus_scheduler_core.config import SchedulerConnectionConfig
from datus_scheduler_core.models import (
    JobRun,
    JobStatus,
    ListJobsResult,
    ListRunsResult,
    RunStatus,
    ScheduledJob,
    SchedulerJobPayload,
)


class BaseSchedulerAdapter(ABC):
    """Abstract base class for all Datus scheduler adapters.

    Responsibilities:
      1. Job creation   — ``submit_job``: generate platform artifacts and register the job.
      2. Job management — ``update_job / trigger_job / pause_job / resume_job / delete_job``.
      3. Status queries — ``get_job / list_jobs / get_job_run / list_job_runs``.

    Identity contract
    -----------------
    * ``job_id`` is the adapter's stable, string-typed mapping to the platform's primary key.
      - Airflow       : ``dag_id``          (user-defined, derived from ``job_name``)
      - DS            : workflow ``code``   (server-generated Long, stored as str)
      - Azkaban       : ``"project/flow"``  (composite, user-defined)
    * ``locator`` stores the raw platform composite key for internal use only.
    * ``submit_job`` returns the first ``ScheduledJob`` carrying the authoritative ``job_id``.
      Callers must persist this value for all subsequent operations.

    Idempotency contract
    --------------------
    * ``submit_job`` is create-only.  When the platform already has a job with the same
      derived ``job_id``, the adapter MUST raise ``SchedulerJobConflictError``.
    * ``update_job`` is update-only.  Renaming (changing ``job_name`` / ``project``) is not
      supported; do ``delete_job`` + ``submit_job`` instead.
    * ``delete_job`` removes both the platform registration and any locally written artifacts
      (e.g. DAG files).  Historical run records may or may not be preserved depending on the
      platform.
    """

    def __init__(self, config: SchedulerConnectionConfig) -> None:
        self.config = config
        self.api_base_url: str = config.api_base_url
        self.timeout: int = config.timeout_seconds

    # ── Platform identity ──────────────────────────────────────────────────

    @abstractmethod
    def platform_name(self) -> str:
        """Return the short platform identifier, e.g. ``'airflow'``."""
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        """Verify that the scheduler is reachable and credentials are valid.

        Returns ``True`` on success.  Raises ``SchedulerConnectionError`` on failure.
        """
        ...

    # ── Job lifecycle ──────────────────────────────────────────────────────

    @abstractmethod
    def submit_job(self, payload: SchedulerJobPayload) -> ScheduledJob:
        """Create and register a new scheduled job on the platform.

        The adapter is responsible for:
        1. Translating ``payload`` into platform-native artifacts (DAG file / process
           definition / flow zip).
        2. Registering those artifacts with the platform.
        3. Generating and returning the authoritative ``job_id``.

        Raises ``SchedulerJobConflictError`` if a job with the same derived ``job_id``
        already exists.  Callers should use ``update_job`` in that case.
        """
        ...

    @abstractmethod
    def trigger_job(self, job_id: str, conf: Optional[Dict[str, Any]] = None) -> JobRun:
        """Trigger an immediate run of a registered job (ignores the schedule).

        Args:
            job_id: The ``job_id`` returned by ``submit_job`` or a query method.
            conf:   Optional run-level configuration dict passed to the executor.

        Returns a ``JobRun`` describing the newly created run instance.
        """
        ...

    @abstractmethod
    def pause_job(self, job_id: str) -> None:
        """Suspend automatic scheduling of the job (keeps the definition intact).

        Raises on failure.
        """
        ...

    @abstractmethod
    def resume_job(self, job_id: str) -> None:
        """Resume automatic scheduling of a paused job.

        Raises on failure.
        """
        ...

    @abstractmethod
    def delete_job(self, job_id: str) -> None:
        """Permanently delete the job definition and its schedule.

        Raises ``SchedulerJobNotFoundError`` if the job does not exist.
        Whether historical run records are preserved depends on the platform.
        """
        ...

    @abstractmethod
    def update_job(self, job_id: str, payload: SchedulerJobPayload) -> ScheduledJob:
        """Replace the definition of an existing job.

        The caller must supply the full updated ``payload``.  The adapter may
        perform an in-place update or a delete-and-recreate, but the ``job_id``
        MUST remain unchanged after this call.

        Renaming (i.e. changing ``payload.job_name`` or ``payload.project``) is
        not supported.  Use ``delete_job`` + ``submit_job`` for that.

        Raises ``SchedulerJobNotFoundError`` if ``job_id`` does not exist.
        """
        ...

    # ── Status queries ─────────────────────────────────────────────────────

    @abstractmethod
    def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        """Return the job's metadata, or ``None`` if it does not exist."""
        ...

    @abstractmethod
    def list_jobs(
        self,
        project: Optional[str] = None,
        status: Optional[JobStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> ListJobsResult:
        """List jobs on the platform.

        Args:
            project: Filter by project scope (DS / Azkaban).  ``None`` returns all.
                     Airflow ignores this parameter.
            status:  Optional status filter.
            limit:   Maximum number of results.
            offset:  Pagination offset.

        Returns:
            ``ListJobsResult`` with ``items`` (the rows for this page) and
            ``total`` (the upstream full count when the platform exposes it,
            or ``None`` when client-side filtering like ``dag_id_prefix``
            makes the server-reported total meaningless).
        """
        ...

    @abstractmethod
    def get_job_run(self, job_id: str, run_id: str) -> Optional[JobRun]:
        """Return details of a specific run instance, or ``None`` if not found."""
        ...

    @abstractmethod
    def list_job_runs(
        self,
        job_id: str,
        status: Optional[RunStatus] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> ListRunsResult:
        """Return run history for a job, newest first.

        Returns:
            ``ListRunsResult`` with ``items`` (run rows for this page) and
            ``total`` (the upstream run count when the platform exposes it).
        """
        ...

    def get_run_log(self, job_id: str, run_id: str) -> str:
        """Return the text log for a run.  Override in adapters that support it."""
        raise NotImplementedError(f"{self.platform_name()} does not support get_run_log")

    # ── Resource cleanup ───────────────────────────────────────────────────

    def close(self) -> None:
        """Release held resources (HTTP session, DB connection, etc.)."""
        return
