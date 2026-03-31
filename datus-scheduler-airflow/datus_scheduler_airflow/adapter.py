# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Airflow scheduler adapter for Datus.

Airflow-specific notes
----------------------
* There is no REST API endpoint to upload or deploy a new DAG.  DAGs are
  Python files that the Airflow scheduler discovers by scanning the
  ``dags_folder``.  This adapter writes generated ``.py`` files into the
  directory specified in ``AirflowConfig.dags_folder`` and then polls the
  REST API until Airflow acknowledges the new DAG.

* ``DELETE /dags/{dag_id}`` (REST API) only removes the DAG record from the
  metadata database; if the ``.py`` file still exists it will be re-imported.
  ``delete_job`` therefore deletes the file first, then calls the API.

* Target: Airflow 2.x (REST API under ``/api/v1``).
  The ``api_base_url`` in the config should end with ``/api/v1``,
  e.g. ``http://localhost:8080/api/v1``.
"""

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from datus_scheduler_core.base import BaseSchedulerAdapter
from datus_scheduler_core.config import AirflowConfig
from datus_scheduler_core.exceptions import (
    SchedulerConnectionError,
    SchedulerException,
    SchedulerJobConflictError,
    SchedulerJobNotFoundError,
    SchedulerTimeoutError,
)
from datus_scheduler_core.models import JobRun, JobStatus, RunStatus, ScheduledJob, SchedulerJobPayload

from datus_scheduler_airflow.dag_template import render_dag_source, render_spark_dag_source, render_sparksql_dag_source

logger = logging.getLogger(__name__)

# Airflow state → RunStatus mapping
_AIRFLOW_STATE_MAP: Dict[str, RunStatus] = {
    "running": RunStatus.RUNNING,
    "success": RunStatus.SUCCESS,
    "failed": RunStatus.FAILED,
    "skipped": RunStatus.SKIPPED,
    "queued": RunStatus.PENDING,
    "scheduled": RunStatus.PENDING,
    "up_for_retry": RunStatus.RUNNING,
    "upstream_failed": RunStatus.FAILED,
    "deferred": RunStatus.PENDING,
}


def _map_run_status(state: Optional[str]) -> RunStatus:
    return _AIRFLOW_STATE_MAP.get(state or "", RunStatus.UNKNOWN)


class AirflowSchedulerAdapter(BaseSchedulerAdapter):
    """Datus scheduler adapter for Apache Airflow 2.x.

    Usage::

        from datus_scheduler_airflow import AirflowSchedulerAdapter
        from datus_scheduler_core.config import AirflowConfig

        config = AirflowConfig(
            name="airflow_prod",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder="/opt/airflow/dags",
        )
        adapter = AirflowSchedulerAdapter(config)
        adapter.test_connection()
    """

    def __init__(self, config: AirflowConfig) -> None:
        if isinstance(config, dict):
            config = AirflowConfig(**config)
        super().__init__(config)
        self._config: AirflowConfig = config
        self._session = httpx.Client(
            base_url=config.api_base_url,
            auth=(config.username, config.password),
            timeout=config.timeout_seconds,
            headers={"Content-Type": "application/json"},
        )

    # ── Internal helpers ───────────────────────────────────────────────────

    @staticmethod
    def _to_dag_id(job_name: str) -> str:
        """Derive a valid Airflow dag_id from a job name.

        Replaces spaces and hyphens with underscores and lowercases everything.
        """
        return job_name.strip().lower().replace(" ", "_").replace("-", "_")

    def _dag_file_path(self, dag_id: str) -> Path:
        return Path(self._config.dags_folder) / f"{dag_id}.py"

    def _write_dag_file(self, dag_id: str, source: str) -> None:
        path = self._dag_file_path(dag_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(source)
            f.flush()
            os.fsync(f.fileno())
        logger.info("Wrote DAG file: %s", path)

    def _remove_dag_file(self, dag_id: str) -> None:
        path = self._dag_file_path(dag_id)
        if path.exists():
            path.unlink()
            logger.info("Removed DAG file: %s", path)
        else:
            logger.debug("DAG file not found (already removed?): %s", path)

    def _wait_for_dag_discovery(self, dag_id: str) -> None:
        """Poll the REST API until Airflow registers the DAG or timeout."""
        timeout = self._config.dag_discovery_timeout
        interval = self._config.dag_discovery_poll_interval
        deadline = time.monotonic() + timeout
        logger.info("Waiting for Airflow to discover DAG '%s' (timeout=%ds)…", dag_id, timeout)
        while time.monotonic() < deadline:
            if self.get_job(dag_id) is not None:
                logger.info("DAG '%s' discovered by Airflow.", dag_id)
                return
            time.sleep(interval)
        raise SchedulerTimeoutError(
            f"DAG '{dag_id}' was not discovered by Airflow within {timeout}s. "
            "Check that dags_folder is correctly mounted and the scheduler is running."
        )

    @staticmethod
    def _extract_schedule(data: dict) -> str | None:
        """Extract schedule as a plain string from Airflow API response.

        ``schedule_interval`` may be a cron string or a dict like
        ``{'__type': 'CronExpression', 'value': '0 10 * * *'}``.
        """
        raw = data.get("schedule_interval")
        if isinstance(raw, dict):
            return raw.get("value")
        if isinstance(raw, str):
            return raw
        return data.get("timetable_description")

    def _build_scheduled_job(self, data: dict) -> ScheduledJob:
        dag_id: str = data["dag_id"]
        is_paused: bool = data.get("is_paused", False)
        return ScheduledJob(
            scheduler_name=self._config.name,
            platform=self.platform_name(),
            job_id=dag_id,
            job_name=data.get("dag_display_name") or dag_id,
            locator={"dag_id": dag_id},
            description=data.get("description"),
            schedule=self._extract_schedule(data),
            status=JobStatus.PAUSED if is_paused else JobStatus.ACTIVE,
            extra={k: v for k, v in data.items() if k not in ("dag_id", "dag_display_name", "is_paused")},
        )

    def _build_job_run(self, data: dict, job_id: str) -> JobRun:
        return JobRun(
            run_id=data["dag_run_id"],
            job_id=job_id,
            status=_map_run_status(data.get("state")),
            started_at=data.get("start_date"),
            ended_at=data.get("end_date"),
            log_url=data.get("log_url"),
            extra={k: v for k, v in data.items() if k not in ("dag_run_id", "state", "start_date", "end_date")},
        )

    # ── Platform identity ──────────────────────────────────────────────────

    def platform_name(self) -> str:
        return "airflow"

    def test_connection(self) -> bool:
        """Verify connectivity and credentials via the Airflow health endpoint."""
        try:
            resp = self._session.get("/health")
            resp.raise_for_status()
            health = resp.json()
            logger.debug("Airflow health: %s", health)
            return True
        except httpx.HTTPStatusError as exc:
            raise SchedulerConnectionError(
                f"Airflow returned HTTP {exc.response.status_code} for /health. "
                "Check api_base_url, username, and password."
            ) from exc
        except Exception as exc:
            raise SchedulerConnectionError(f"Failed to connect to Airflow: {exc}") from exc

    # ── Job lifecycle ──────────────────────────────────────────────────────

    def submit_job(self, payload: SchedulerJobPayload) -> ScheduledJob:
        """Create a new Airflow DAG from the payload and wait for Airflow to pick it up.

        Steps:
        1. Derive ``dag_id`` from ``payload.job_name``.
        2. Conflict-check via REST API.
        3. Render and write the DAG Python file to ``dags_folder``.
        4. Poll until Airflow discovers the DAG.
        5. Return ``ScheduledJob``.
        """
        job_type = (payload.extra or {}).get("job_type", "sql")
        dag_id = self._to_dag_id(payload.job_name)

        # Conflict detection
        if self.get_job(dag_id) is not None:
            raise SchedulerJobConflictError(dag_id, self.platform_name())

        # Render DAG source based on job_type
        if job_type == "spark":
            spark_script = (payload.extra or {}).get("spark_script")
            if not spark_script:
                raise SchedulerException(
                    "AirflowSchedulerAdapter.submit_job with job_type='spark' requires extra['spark_script']."
                )
            source = render_spark_dag_source(
                dag_id=dag_id,
                job_name=payload.job_name,
                spark_script=spark_script,
                spark_master=(payload.extra or {}).get("spark_master", "local[*]"),
                schedule=payload.schedule,
                start_date=payload.start_date,
                description=payload.description,
            )
        elif job_type == "sparksql":
            sparksql = (payload.extra or {}).get("sparksql")
            if not sparksql:
                raise SchedulerException(
                    "AirflowSchedulerAdapter.submit_job with job_type='sparksql' requires extra['sparksql']."
                )
            source = render_sparksql_dag_source(
                dag_id=dag_id,
                job_name=payload.job_name,
                sql=sparksql,
                spark_master=(payload.extra or {}).get("spark_master", "local[*]"),
                schedule=payload.schedule,
                start_date=payload.start_date,
                end_date=payload.end_date,
                description=payload.description,
            )
        else:
            if not payload.sql:
                raise SchedulerException("AirflowSchedulerAdapter.submit_job requires payload.sql to be set.")
            source = render_dag_source(
                dag_id=dag_id,
                job_name=payload.job_name,
                sql=payload.sql,
                db_connection=payload.db_connection,
                schedule=payload.schedule,
                start_date=payload.start_date,
                end_date=payload.end_date,
                description=payload.description,
            )

        self._write_dag_file(dag_id, source)

        try:
            self._wait_for_dag_discovery(dag_id)
        except SchedulerTimeoutError:
            # Keep the file — Airflow will eventually discover it.
            logger.warning(
                "DAG '%s' not yet discovered by Airflow (timeout=%ds). "
                "The DAG file was kept at %s and will be picked up on the next scheduler scan.",
                dag_id,
                self._config.dag_discovery_timeout,
                self._dag_file_path(dag_id),
            )
            return ScheduledJob(
                scheduler_name=self._config.name,
                platform=self.platform_name(),
                job_id=dag_id,
                job_name=payload.job_name,
                locator={"dag_id": dag_id},
                description=payload.description,
                schedule=payload.schedule,
                status=JobStatus.UNKNOWN,
            )

        job = self.get_job(dag_id)
        if job is None:
            raise SchedulerException(f"DAG '{dag_id}' disappeared immediately after discovery.")
        return job

    def trigger_job(self, job_id: str, conf: Optional[Dict[str, Any]] = None) -> JobRun:
        """Trigger an immediate DAG run via POST /dags/{dag_id}/dagRuns."""
        body: Dict[str, Any] = {"conf": conf or {}}
        resp = self._session.post(f"/dags/{job_id}/dagRuns", json=body)
        if resp.status_code == 404:
            raise SchedulerJobNotFoundError(job_id, self.platform_name())
        resp.raise_for_status()
        return self._build_job_run(resp.json(), job_id)

    def pause_job(self, job_id: str) -> None:
        resp = self._session.patch(f"/dags/{job_id}", json={"is_paused": True})
        if resp.status_code == 404:
            raise SchedulerJobNotFoundError(job_id, self.platform_name())
        resp.raise_for_status()

    def resume_job(self, job_id: str) -> None:
        resp = self._session.patch(f"/dags/{job_id}", json={"is_paused": False})
        if resp.status_code == 404:
            raise SchedulerJobNotFoundError(job_id, self.platform_name())
        resp.raise_for_status()

    def _wait_for_dag_inactive(self, dag_id: str, max_wait: int = 30, interval: int = 3) -> bool:
        """Poll until Airflow marks the DAG inactive (is_active=False) or file is not found."""
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            resp = self._session.get(f"/dags/{dag_id}")
            if resp.status_code == 404:
                return True
            if resp.status_code == 200:
                data = resp.json()
                if not data.get("is_active", True):
                    return True
            time.sleep(interval)
        return False

    def delete_job(self, job_id: str) -> None:
        """Delete a DAG: pause it, remove the file, wait for Airflow to mark it inactive, then call DELETE API.

        Airflow 2.x requires the DAG to be inactive (file removed + scheduler scanned) before
        the DELETE API will accept the request.
        """
        if self.get_job(job_id) is None:
            raise SchedulerJobNotFoundError(job_id, self.platform_name())

        # Step 1: Pause to prevent new runs while we clean up
        try:
            self._session.patch(f"/dags/{job_id}", json={"is_paused": True})
        except Exception:
            pass

        # Step 2: Remove the DAG file so the scheduler won't re-import it
        self._remove_dag_file(job_id)

        # Step 3: Wait for the scheduler to mark the DAG as inactive
        self._wait_for_dag_inactive(job_id, max_wait=self._config.dag_discovery_timeout)

        # Step 4: Remove the DB record.
        # Airflow's DELETE endpoint checks dag_bag (in-memory cache) which may refresh
        # slower than DagModel.is_active. Retry with backoff to give dag_bag time to clear.
        delete_succeeded = False
        for attempt in range(6):
            resp = self._session.delete(f"/dags/{job_id}")
            if resp.status_code in (200, 204, 404):
                delete_succeeded = True
                break
            if resp.status_code == 400:
                if attempt < 5:
                    logger.debug("DELETE /dags/%s returned 400 (attempt %d), dag_bag may not be refreshed yet. Retrying in 5s…", job_id, attempt + 1)
                    time.sleep(5)
                    continue
                else:
                    # dag_bag still has the entry after all retries, but the file is gone.
                    # The DAG cannot run again; accept this as a successful delete.
                    logger.debug("DELETE /dags/%s returned 400 after all retries; DAG file removed, treating as deleted.", job_id)
                    break
            resp.raise_for_status()

        if delete_succeeded:
            logger.info("Deleted DAG '%s' from Airflow.", job_id)
        else:
            logger.warning(
                "Could not fully remove DAG '%s' metadata from Airflow (dag_bag refresh pending). "
                "The DAG file has been deleted and will not run again.",
                job_id,
            )

    def update_job(self, job_id: str, payload: SchedulerJobPayload) -> ScheduledJob:
        """Re-render and overwrite the DAG file, then wait for Airflow to reload it."""
        if self.get_job(job_id) is None:
            raise SchedulerJobNotFoundError(job_id, self.platform_name())

        job_type = (payload.extra or {}).get("job_type", "sql")

        if job_type == "spark":
            spark_script = (payload.extra or {}).get("spark_script")
            if not spark_script:
                raise SchedulerException(
                    "AirflowSchedulerAdapter.update_job with job_type='spark' requires extra['spark_script']."
                )
            source = render_spark_dag_source(
                dag_id=job_id,
                job_name=payload.job_name,
                spark_script=spark_script,
                spark_master=(payload.extra or {}).get("spark_master", "local[*]"),
                schedule=payload.schedule,
                start_date=payload.start_date,
                description=payload.description,
            )
        elif job_type == "sparksql":
            sparksql = (payload.extra or {}).get("sparksql")
            if not sparksql:
                raise SchedulerException(
                    "AirflowSchedulerAdapter.update_job with job_type='sparksql' requires extra['sparksql']."
                )
            source = render_sparksql_dag_source(
                dag_id=job_id,
                job_name=payload.job_name,
                sql=sparksql,
                spark_master=(payload.extra or {}).get("spark_master", "local[*]"),
                schedule=payload.schedule,
                start_date=payload.start_date,
                end_date=payload.end_date,
                description=payload.description,
            )
        else:
            if not payload.sql:
                raise SchedulerException("AirflowSchedulerAdapter.update_job requires payload.sql to be set.")
            source = render_dag_source(
                dag_id=job_id,
                job_name=payload.job_name,
                sql=payload.sql,
                db_connection=payload.db_connection,
                schedule=payload.schedule,
                start_date=payload.start_date,
                end_date=payload.end_date,
                description=payload.description,
            )
        self._write_dag_file(job_id, source)
        # Airflow reloads existing DAG files automatically; wait for a brief moment
        time.sleep(self._config.dag_discovery_poll_interval)

        job = self.get_job(job_id)
        if job is None:
            raise SchedulerException(f"DAG '{job_id}' not found after update.")
        return job

    # ── Status queries ─────────────────────────────────────────────────────

    def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        resp = self._session.get(f"/dags/{job_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return self._build_scheduled_job(resp.json())

    def list_jobs(
        self,
        project: Optional[str] = None,  # Airflow has no project concept; ignored
        status: Optional[JobStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[ScheduledJob]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if status == JobStatus.PAUSED:
            params["only_active"] = "false"
        resp = self._session.get("/dags", params=params)
        resp.raise_for_status()
        data = resp.json()
        jobs = [self._build_scheduled_job(d) for d in data.get("dags", [])]
        if status is not None:
            jobs = [j for j in jobs if j.status == status]
        return jobs

    def get_job_run(self, job_id: str, run_id: str) -> Optional[JobRun]:
        resp = self._session.get(f"/dags/{job_id}/dagRuns/{run_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return self._build_job_run(resp.json(), job_id)

    def list_job_runs(
        self,
        job_id: str,
        status: Optional[RunStatus] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[JobRun]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset, "order_by": "-execution_date"}
        resp = self._session.get(f"/dags/{job_id}/dagRuns", params=params)
        if resp.status_code == 404:
            raise SchedulerJobNotFoundError(job_id, self.platform_name())
        resp.raise_for_status()
        runs = [self._build_job_run(d, job_id) for d in resp.json().get("dag_runs", [])]
        if status is not None:
            runs = [r for r in runs if r.status == status]
        return runs

    def get_run_log(self, job_id: str, run_id: str) -> str:
        """Fetch the log text for the first task instance of a DAG run."""
        # Get task instances for the run
        resp = self._session.get(f"/dags/{job_id}/dagRuns/{run_id}/taskInstances")
        resp.raise_for_status()
        task_instances = resp.json().get("task_instances", [])
        if not task_instances:
            return f"No task instances found for run {run_id}."
        task_id = task_instances[0]["task_id"]
        try_number = task_instances[0].get("try_number", 1)
        log_resp = self._session.get(
            f"/dags/{job_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
        )
        log_resp.raise_for_status()
        return log_resp.text

    # ── Resource cleanup ───────────────────────────────────────────────────

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass
