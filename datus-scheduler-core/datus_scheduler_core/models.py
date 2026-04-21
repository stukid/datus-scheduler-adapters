# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class PaginatedScheduledResult(BaseModel, Generic[T]):
    """Generic paginated envelope for scheduler list_* APIs.

    * ``items``: rows for the requested ``(limit, offset)`` window. Always
      a list; empty is ``[]``.
    * ``total``: upstream full count when known (Airflow ``total_entries``).
      ``None`` when unknown or meaningless — e.g. when client-side filtering
      (``dag_id_prefix``) shortens the list and the server's total no longer
      reflects what the caller sees.
    """

    model_config = ConfigDict(extra="allow")

    items: List[T] = Field(default_factory=list)
    total: Optional[int] = None


class JobStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    DELETED = "deleted"
    UNKNOWN = "unknown"


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    PENDING = "pending"
    KILLED = "killed"
    UNKNOWN = "unknown"


class SchedulerJobPayload(BaseModel):
    """The task definition that Datus submits to a scheduler platform.

    Two execution modes (at least one required):
      - ``sql`` + ``db_type`` + ``db_connection``: the adapter embeds these into the
        scheduled job so the platform runs the SQL directly.
      - ``datus_command``: a full Datus CLI invocation that the platform executes as a
        shell command (e.g. ``datus run --question "..."``)
    """

    job_name: str = Field(..., description="Human-readable name; used to derive the platform job ID.")
    description: Optional[str] = Field(default=None)

    # Scope — DS / Azkaban require a project; Airflow ignores this field.
    project: Optional[str] = Field(
        default=None,
        description="Target project (DS/Azkaban required; Airflow: ignored). "
        "Falls back to config.default_project when None.",
    )

    # Execution content — sql mode
    sql: Optional[str] = Field(default=None, description="SQL to execute.")
    db_type: Optional[str] = Field(default=None, description="Database type, e.g. mysql / postgresql / duckdb.")
    db_connection: Optional[Dict[str, Any]] = Field(default=None, description="DB connection parameters.")

    # Execution content — CLI mode
    datus_command: Optional[str] = Field(
        default=None, description="Full datus CLI command to run (alternative to sql mode)."
    )

    # Schedule
    schedule: Optional[str] = Field(default=None, description="Cron expression. None = manual trigger only.")
    timezone: str = Field(default="UTC")
    start_date: Optional[datetime] = Field(default=None)
    end_date: Optional[datetime] = Field(default=None)

    # Adapter-specific passthrough
    extra: Dict[str, Any] = Field(default_factory=dict)


class ScheduledJob(BaseModel):
    """Platform-agnostic view of a registered scheduled job."""

    scheduler_name: str = Field(..., description="Scheduler instance name from agent.yml (e.g. 'airflow_prod').")
    platform: str = Field(..., description="Platform type, e.g. 'airflow'.")
    job_id: str = Field(
        ...,
        description=(
            "Adapter's stable mapping to the platform's primary key. "
            "For Airflow: dag_id. For DS: workflow code (str). For Azkaban: 'project/flow'."
        ),
    )
    job_name: str
    project: Optional[str] = Field(default=None)
    locator: Dict[str, str] = Field(
        default_factory=dict,
        description="Platform-native composite key, e.g. {'dag_id': '...'} or {'workflow_code': '...'}.",
    )
    description: Optional[str] = None
    schedule: Optional[str] = None
    status: JobStatus = JobStatus.UNKNOWN
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    extra: Dict[str, Any] = Field(default_factory=dict, description="Raw platform metadata.")


class JobRun(BaseModel):
    """A single execution instance of a scheduled job."""

    run_id: str = Field(..., description="Platform-internal run identifier.")
    job_id: str
    status: RunStatus = RunStatus.UNKNOWN
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    log_url: Optional[str] = Field(default=None, description="Deep-link to the platform's log viewer.")
    extra: Dict[str, Any] = Field(default_factory=dict)


# Concrete envelope types — named subclasses of the generic so call sites
# can be explicit about what they return without writing the generic
# parameter every time.


class ListJobsResult(PaginatedScheduledResult[ScheduledJob]):
    """Paginated response from ``BaseSchedulerAdapter.list_jobs``."""


class ListRunsResult(PaginatedScheduledResult[JobRun]):
    """Paginated response from ``BaseSchedulerAdapter.list_job_runs``."""
