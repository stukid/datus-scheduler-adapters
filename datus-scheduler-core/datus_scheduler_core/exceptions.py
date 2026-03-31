# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.


class SchedulerException(Exception):
    """Base exception for all scheduler adapter errors."""


class SchedulerJobConflictError(SchedulerException):
    """Raised when attempting to create a job that already exists."""

    def __init__(self, job_id: str, platform: str) -> None:
        super().__init__(f"Job '{job_id}' already exists on {platform}. Use update_job() instead.")
        self.job_id = job_id
        self.platform = platform


class SchedulerJobNotFoundError(SchedulerException):
    """Raised when a referenced job is not found on the platform."""

    def __init__(self, job_id: str, platform: str) -> None:
        super().__init__(f"Job '{job_id}' not found on {platform}.")
        self.job_id = job_id
        self.platform = platform


class SchedulerConnectionError(SchedulerException):
    """Raised when the connection to the scheduler platform fails."""


class SchedulerTimeoutError(SchedulerException):
    """Raised when waiting for a platform operation times out."""
