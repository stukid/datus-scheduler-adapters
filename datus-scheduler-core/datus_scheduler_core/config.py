# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

import os
import re
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, model_validator

# Filesystem-safe project name. Mirrors the regex used by Datus'
# ``_validate_project_name`` so a name derived from ``agent.project_name``
# always round-trips through this config. Matched with ``fullmatch`` so a
# trailing newline is rejected (``re.match`` would accept it).
_PROJECT_NAME_RE = re.compile(r"[A-Za-z0-9_.\-]+")

# dag_id_prefix either is empty (prefixing disabled) or must end with the
# ``__`` separator. Without that separator, ``startswith(prefix)`` during
# list_jobs would let a prefix of ``team`` match ``team_work`` and leak
# across tenants. Matched with ``fullmatch``.
_DAG_ID_PREFIX_RE = re.compile(r"|[a-z0-9_]+__")


class SchedulerConnectionConfig(BaseModel):
    """Base connection config for all scheduler adapters.

    Sub-classes should add platform-specific authentication fields.
    """

    name: str = Field(..., description="Scheduler instance name (from the 'scheduler' section in agent.yml).")
    type: str = Field(..., description="Platform type, e.g. 'airflow' / 'dolphinscheduler' / 'azkaban'.")
    api_base_url: str = Field(
        ..., description="REST API base URL, including version path, e.g. http://host:8080/api/v1."
    )
    timeout_seconds: int = Field(default=30, description="HTTP request timeout in seconds.")
    extra: dict = Field(default_factory=dict, description="Platform-specific passthrough parameters.")


class AirflowConfig(SchedulerConnectionConfig):
    """Connection config for Apache Airflow (2.x / 3.x).

    Two orthogonal concerns, previously coupled through a single
    ``project_name`` field:

    1. **DAG file-path namespacing** — where the adapter drops the
       ``.py`` files it generates. Driven by ``project_name``: when
       ``dags_folder_root`` is set, DAG files land in
       ``{dags_folder_root}/{project_name}/`` so multiple Datus
       instances writing to the same Airflow cluster never collide at
       the filesystem layer. Datus auto-injects
       ``agent.project_name`` here; users rarely need to set it by
       hand. Airflow itself has no "project" concept — this is
       purely a Datus-side collision guard.

    2. **dag_id prefixing** (opt-in) — whether every ``dag_id`` this
       instance creates is prefixed so ``list_jobs`` / ``get_job`` see
       only DAGs belonging to this tenant. Controlled by
       ``dag_id_prefix``; defaults to the empty string so DAG names on
       the Airflow UI stay clean. Teams who share an Airflow cluster
       and want list-level isolation set an explicit value here.

    Two deployment modes for the DAG folder:

    * **Shared-root mode** (recommended for multi-instance deployments):
      set ``dags_folder_root`` (or env ``DATUS_AIRFLOW_DAGS_ROOT``) to
      Airflow's ``AIRFLOW__CORE__DAGS_FOLDER``. Datus auto-injects
      ``project_name`` so files land in ``{dags_folder_root}/{project_name}/``.

    * **Legacy / explicit mode**: set ``dags_folder`` to a fixed
      absolute path. Suitable when the operator wants full control
      over where DAG files land.

    Exactly one of ``dags_folder`` / ``dags_folder_root`` must resolve
    at validation time.
    """

    username: str = Field(..., description="Airflow webserver username.")
    password: str = Field(..., description="Airflow webserver password.")

    # --- DAG file location --------------------------------------------------
    dags_folder_root: Optional[str] = Field(
        default=None,
        description=(
            "Parent directory shared by all Datus instances. Typically equal to "
            "Airflow's AIRFLOW__CORE__DAGS_FOLDER. When set, the effective "
            "dags_folder becomes '{dags_folder_root}/{project_name}'. "
            "Falls back to env DATUS_AIRFLOW_DAGS_ROOT when unset. "
            "Mutually exclusive with dags_folder."
        ),
    )
    dags_folder: Optional[str] = Field(
        default=None,
        description=(
            "Absolute local path to the directory where DAG files are written. "
            "Mutually exclusive with dags_folder_root. "
            "Must be the same directory monitored by the Airflow scheduler "
            "(or a sub-directory thereof), typically mounted as a shared volume."
        ),
    )

    # --- Namespacing ------------------------------------------------------
    project_name: Optional[str] = Field(
        default=None,
        description=(
            "Datus workspace identifier. Used as the subdirectory name "
            "under dags_folder_root so DAG files from different Datus "
            "instances don't collide on disk. Datus auto-injects "
            "agent.project_name at adapter construction; users rarely "
            "need to set this by hand. Required when dags_folder_root "
            "is in play (explicitly or via env var). Must match "
            "^[A-Za-z0-9_.\\-]+$."
        ),
    )
    dag_id_prefix: Optional[str] = Field(
        default=None,
        description=(
            "Prefix prepended to every dag_id written by this adapter. "
            "Defaults to the empty string so DAG names on the Airflow UI "
            "stay clean. Set explicitly (e.g. 'team_a__') to isolate "
            "this adapter's DAGs from other tenants sharing the same "
            "Airflow cluster — list_jobs / get_job / list_runs then "
            "return only DAGs whose id starts with the prefix. Must "
            "match ^[a-z0-9_]+__$ or be empty."
        ),
    )

    # --- Discovery polling --------------------------------------------------
    dag_discovery_timeout: int = Field(
        default=60,
        description="Seconds to wait for Airflow to discover a newly written DAG file.",
    )
    dag_discovery_poll_interval: int = Field(
        default=5,
        description="Polling interval (seconds) when waiting for DAG discovery.",
    )

    @model_validator(mode="after")
    def _resolve_paths(self) -> "AirflowConfig":
        # Reject conflicting path specs up-front so misconfiguration surfaces
        # at config load time rather than on first submit_job.
        if self.dags_folder and self.dags_folder_root:
            raise ValueError(
                "AirflowConfig: set either 'dags_folder' (full path) or "
                "'dags_folder_root' (parent dir + project_name), not both."
            )

        if self.project_name is not None and not _PROJECT_NAME_RE.fullmatch(self.project_name):
            raise ValueError(f"AirflowConfig.project_name {self.project_name!r} must match {_PROJECT_NAME_RE.pattern}.")

        # Even though the regex allows '.' and '-', path traversal via '.', '..'
        # or '..foo' would escape dags_folder_root when joined. Reject these
        # explicitly rather than relying on later Path normalization.
        if self.project_name is not None and (
            self.project_name in (".", "..")
            or self.project_name.startswith(".")
            or ".." in self.project_name
        ):
            raise ValueError(
                f"AirflowConfig.project_name {self.project_name!r} is not allowed: "
                "must not be '.', '..', start with '.', or contain '..'."
            )

        if self.dag_id_prefix is not None and not _DAG_ID_PREFIX_RE.fullmatch(self.dag_id_prefix):
            raise ValueError(
                f"AirflowConfig.dag_id_prefix {self.dag_id_prefix!r} must be empty or match "
                f"[a-z0-9_]+__ (trailing double-underscore is required as a tenant separator)."
            )

        # Resolve effective dags_folder when only dags_folder_root is given.
        if not self.dags_folder:
            root = self.dags_folder_root or os.environ.get("DATUS_AIRFLOW_DAGS_ROOT")
            if not root:
                raise ValueError(
                    "AirflowConfig requires one of: 'dags_folder' (full path), "
                    "'dags_folder_root' (parent dir), or env DATUS_AIRFLOW_DAGS_ROOT."
                )
            if not self.project_name:
                raise ValueError(
                    "AirflowConfig.project_name is required when using "
                    "dags_folder_root (or DATUS_AIRFLOW_DAGS_ROOT). "
                    "Datus callers should pass agent_config.project_name."
                )
            self.dags_folder = str(Path(root) / self.project_name)

        # ``project_name`` is a filesystem-namespace concern ONLY — it no
        # longer auto-derives a ``dag_id_prefix``. dag_id prefixing is
        # opt-in: teams who share an Airflow cluster and want list-level
        # isolation set ``dag_id_prefix`` explicitly (e.g. "team_a__").
        # Unset → empty string → clean dag_ids, unfiltered list_jobs.
        if self.dag_id_prefix is None:
            self.dag_id_prefix = ""

        return self


class DolphinSchedulerConfig(SchedulerConnectionConfig):
    """Connection config for Apache DolphinScheduler."""

    token: str = Field(..., description="API token.")
    default_project: Optional[str] = Field(
        default=None,
        description=(
            "Default project identifier (string form of the project code, e.g. '12345678'). "
            "Used as fallback when SchedulerJobPayload.project is None."
        ),
    )


class AzkabanConfig(SchedulerConnectionConfig):
    """Connection config for Azkaban."""

    username: str
    password: str
    session_id: Optional[str] = Field(default=None, description="Cached session ID after login.")
    default_project: Optional[str] = Field(
        default=None,
        description="Default Azkaban project name used when SchedulerJobPayload.project is None.",
    )
