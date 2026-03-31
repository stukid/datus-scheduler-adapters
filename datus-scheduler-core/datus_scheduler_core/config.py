# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

from typing import Optional

from pydantic import BaseModel, Field


class SchedulerConnectionConfig(BaseModel):
    """Base connection config for all scheduler adapters.

    Sub-classes should add platform-specific authentication fields.
    """

    name: str = Field(..., description="Scheduler instance name (from the 'scheduler' section in agent.yml).")
    type: str = Field(..., description="Platform type, e.g. 'airflow' / 'dolphinscheduler' / 'azkaban'.")
    api_base_url: str = Field(..., description="REST API base URL, including version path, e.g. http://host:8080/api/v1.")
    timeout_seconds: int = Field(default=30, description="HTTP request timeout in seconds.")
    extra: dict = Field(default_factory=dict, description="Platform-specific passthrough parameters.")


class AirflowConfig(SchedulerConnectionConfig):
    """Connection config for Apache Airflow (2.x / 3.x)."""

    username: str = Field(..., description="Airflow webserver username.")
    password: str = Field(..., description="Airflow webserver password.")
    dags_folder: str = Field(
        ...,
        description=(
            "Absolute local path to the directory where DAG files should be written. "
            "This path must be the same directory that Airflow's scheduler monitors "
            "(or a sub-directory thereof), typically mounted as a shared volume."
        ),
    )
    dag_discovery_timeout: int = Field(
        default=60,
        description="Seconds to wait for Airflow to discover a newly written DAG file.",
    )
    dag_discovery_poll_interval: int = Field(
        default=5,
        description="Polling interval (seconds) when waiting for DAG discovery.",
    )


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
