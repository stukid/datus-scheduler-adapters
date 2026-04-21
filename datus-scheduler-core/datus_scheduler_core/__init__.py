# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

from datus_scheduler_core.base import BaseSchedulerAdapter
from datus_scheduler_core.config import AirflowConfig, AzkabanConfig, DolphinSchedulerConfig, SchedulerConnectionConfig
from datus_scheduler_core.exceptions import (
    SchedulerConnectionError,
    SchedulerException,
    SchedulerJobConflictError,
    SchedulerJobNotFoundError,
    SchedulerTimeoutError,
)
from datus_scheduler_core.models import (
    JobRun,
    JobStatus,
    ListJobsResult,
    ListRunsResult,
    PaginatedScheduledResult,
    RunStatus,
    ScheduledJob,
    SchedulerJobPayload,
)
from datus_scheduler_core.registry import SchedulerAdapterMetadata, SchedulerAdapterRegistry, scheduler_registry

__version__ = "0.1.0"

__all__ = [
    # Base
    "BaseSchedulerAdapter",
    # Config
    "SchedulerConnectionConfig",
    "AirflowConfig",
    "DolphinSchedulerConfig",
    "AzkabanConfig",
    # Exceptions
    "SchedulerException",
    "SchedulerJobConflictError",
    "SchedulerJobNotFoundError",
    "SchedulerConnectionError",
    "SchedulerTimeoutError",
    # Models
    "SchedulerJobPayload",
    "ScheduledJob",
    "JobRun",
    "JobStatus",
    "RunStatus",
    "PaginatedScheduledResult",
    "ListJobsResult",
    "ListRunsResult",
    # Registry
    "SchedulerAdapterRegistry",
    "SchedulerAdapterMetadata",
    "scheduler_registry",
]
