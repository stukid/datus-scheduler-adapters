# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Integration tests for datus-scheduler-airflow adapter against a live Airflow instance.

Prerequisite: run ``docker compose up -d`` from the
``datus-scheduler-airflow/tests/integration/`` directory and wait
~60 seconds for Airflow to become healthy.

Mark: ``@pytest.mark.integration``
Run:  ``uv run pytest datus-scheduler-airflow/tests/integration/ -v -m integration``
"""

import os
import time
import uuid
from typing import List

import pytest
from datus_scheduler_airflow.adapter import AirflowSchedulerAdapter
from datus_scheduler_core.exceptions import (
    SchedulerJobConflictError,
    SchedulerJobNotFoundError,
)
from datus_scheduler_core.models import JobStatus, RunStatus, SchedulerJobPayload

pytestmark = pytest.mark.integration

# ── Helpers ────────────────────────────────────────────────────────────────


def _unique_job_name(prefix: str = "datus_test") -> str:
    """Generate a unique job name to avoid cross-test collisions."""
    short_id = uuid.uuid4().hex[:8]
    return f"{prefix}_{short_id}"


def _wait_for_run_terminal(
    adapter: AirflowSchedulerAdapter,
    job_id: str,
    run_id: str,
    max_wait: int = 60,
    poll: int = 5,
) -> RunStatus:
    """Poll until the run reaches a terminal state or timeout."""
    terminal = {RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.KILLED}
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        run = adapter.get_job_run(job_id, run_id)
        if run and run.status in terminal:
            return run.status
        time.sleep(poll)
    run = adapter.get_job_run(job_id, run_id)
    return run.status if run else RunStatus.UNKNOWN


# ── Tests ──────────────────────────────────────────────────────────────────


class TestConnection:
    def test_connection_succeeds(self, adapter: AirflowSchedulerAdapter) -> None:
        assert adapter.test_connection() is True


class TestSubmitJob:
    def test_submit_creates_dag(self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str]) -> None:
        name = _unique_job_name("submit")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1 AS value",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
            description="Integration test DAG",
        )

        job = adapter.submit_job(payload)
        cleanup_dag.append(job.job_id)

        assert job.job_id == adapter._to_dag_id(name)
        assert job.platform == "airflow"
        assert job.status in (JobStatus.ACTIVE, JobStatus.PAUSED)
        assert job.locator.get("dag_id") == job.job_id

    def test_submit_conflict_raises(self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str]) -> None:
        name = _unique_job_name("conflict")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )

        job = adapter.submit_job(payload)
        cleanup_dag.append(job.job_id)

        with pytest.raises(SchedulerJobConflictError):
            adapter.submit_job(payload)

    def test_submit_dag_file_on_disk(
        self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str], dags_folder
    ) -> None:
        name = _unique_job_name("file_check")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 42 AS answer",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )

        job = adapter.submit_job(payload)
        cleanup_dag.append(job.job_id)

        dag_file = dags_folder / f"{job.job_id}.py"
        assert dag_file.exists(), f"DAG file not found: {dag_file}"
        content = dag_file.read_text()
        assert "SELECT 42 AS answer" in content


class TestGetJob:
    def test_get_existing_job(self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str]) -> None:
        name = _unique_job_name("get_job")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )
        submitted = adapter.submit_job(payload)
        cleanup_dag.append(submitted.job_id)

        fetched = adapter.get_job(submitted.job_id)
        assert fetched is not None
        assert fetched.job_id == submitted.job_id

    def test_get_nonexistent_returns_none(self, adapter: AirflowSchedulerAdapter) -> None:
        result = adapter.get_job("this_dag_does_not_exist_xyz_12345")
        assert result is None


class TestListJobs:
    def test_list_returns_jobs(self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str]) -> None:
        name = _unique_job_name("list")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )
        job = adapter.submit_job(payload)
        cleanup_dag.append(job.job_id)

        jobs = adapter.list_jobs(limit=100)
        job_ids = [j.job_id for j in jobs.items]
        assert job.job_id in job_ids


class TestPauseResume:
    def test_pause_and_resume(self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str]) -> None:
        name = _unique_job_name("pause_resume")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )
        job = adapter.submit_job(payload)
        cleanup_dag.append(job.job_id)

        adapter.pause_job(job.job_id)
        paused = adapter.get_job(job.job_id)
        assert paused is not None
        assert paused.status == JobStatus.PAUSED

        adapter.resume_job(job.job_id)
        resumed = adapter.get_job(job.job_id)
        assert resumed is not None
        assert resumed.status == JobStatus.ACTIVE

    def test_pause_nonexistent_raises(self, adapter: AirflowSchedulerAdapter) -> None:
        with pytest.raises(SchedulerJobNotFoundError):
            adapter.pause_job("nonexistent_dag_xyz_99999")


class TestTriggerJob:
    def test_trigger_creates_run(self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str]) -> None:
        name = _unique_job_name("trigger")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1 AS v",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )
        job = adapter.submit_job(payload)
        cleanup_dag.append(job.job_id)

        run = adapter.trigger_job(job.job_id)
        assert run.run_id
        assert run.job_id == job.job_id
        assert run.status in (
            RunStatus.RUNNING,
            RunStatus.PENDING,
            RunStatus.SUCCESS,
            RunStatus.QUEUED if hasattr(RunStatus, "QUEUED") else RunStatus.PENDING,
        )

    def test_trigger_and_wait_success(self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str]) -> None:
        """Full end-to-end: submit → trigger → wait for terminal state."""
        name = _unique_job_name("e2e")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1 AS result",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
            description="E2E integration test",
        )
        job = adapter.submit_job(payload)
        cleanup_dag.append(job.job_id)

        run = adapter.trigger_job(job.job_id)
        final_status = _wait_for_run_terminal(adapter, job.job_id, run.run_id, max_wait=120)

        assert final_status == RunStatus.SUCCESS, (
            f"DAG run ended with {final_status!r} instead of SUCCESS. Check the Airflow logs for details."
        )


class TestListJobRuns:
    def test_list_runs_after_trigger(self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str]) -> None:
        name = _unique_job_name("list_runs")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )
        job = adapter.submit_job(payload)
        cleanup_dag.append(job.job_id)

        adapter.trigger_job(job.job_id)
        time.sleep(3)

        runs = adapter.list_job_runs(job.job_id)
        assert len(runs.items) >= 1
        assert runs.items[0].job_id == job.job_id


class TestUpdateJob:
    def test_update_changes_sql(self, adapter: AirflowSchedulerAdapter, cleanup_dag: List[str], dags_folder) -> None:
        name = _unique_job_name("update")
        original = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1 AS original",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )
        job = adapter.submit_job(original)
        cleanup_dag.append(job.job_id)

        updated_payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 2 AS updated",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )
        adapter.update_job(job.job_id, updated_payload)

        dag_file = dags_folder / f"{job.job_id}.py"
        content = dag_file.read_text()
        assert "SELECT 2 AS updated" in content
        assert "SELECT 1 AS original" not in content


class TestDeleteJob:
    def test_delete_removes_dag(self, adapter: AirflowSchedulerAdapter, dags_folder) -> None:
        name = _unique_job_name("delete")
        payload = SchedulerJobPayload(
            job_name=name,
            sql="SELECT 1",
            db_connection={"url": "sqlite:///:memory:"},
            schedule=None,
        )
        job = adapter.submit_job(payload)

        adapter.delete_job(job.job_id)

        # Primary invariant: the DAG file no longer exists (DAG cannot run again)
        dag_file = dags_folder / f"{job.job_id}.py"
        assert not dag_file.exists(), f"DAG file should have been removed: {dag_file}"

        # Secondary check: either fully removed from DB (None) OR marked inactive
        remaining = adapter.get_job(job.job_id)
        if remaining is not None:
            # DB record may linger until next scheduler scan; verify it's marked inactive
            assert remaining.extra.get("is_active") is False, (
                f"DAG '{job.job_id}' still active after delete: {remaining.extra}"
            )

    def test_delete_nonexistent_raises(self, adapter: AirflowSchedulerAdapter) -> None:
        with pytest.raises(SchedulerJobNotFoundError):
            adapter.delete_job("nonexistent_dag_xyz_88888")


# ── Multi-tenant shared-root integration ─────────────────────────────────


class TestMultiTenantMode:
    """End-to-end: two Datus instances share one Airflow via dags_folder_root.

    Each instance derives its own subdirectory and dag_id prefix from
    ``project_name``, so submitting the same ``job_name`` from both instances
    produces two distinct DAGs that don't collide and that ``list_jobs``
    correctly scopes to their respective owners.
    """

    def _make_tenant_adapter(
        self,
        dags_folder_root,
        airflow_url: str,
        airflow_user: str,
        airflow_password: str,
        project_name: str,
    ) -> AirflowSchedulerAdapter:
        from datus_scheduler_core.config import AirflowConfig

        cfg = AirflowConfig(
            name=f"airflow_test_{project_name}",
            type="airflow",
            api_base_url=airflow_url,
            username=airflow_user,
            password=airflow_password,
            dags_folder_root=str(dags_folder_root),
            project_name=project_name,
            dag_id_prefix=f"{project_name}__",
            dag_discovery_timeout=90,
            dag_discovery_poll_interval=5,
        )
        return AirflowSchedulerAdapter(cfg)

    def test_two_tenants_dont_collide(self, dags_folder, airflow_ready: bool) -> None:
        airflow_url = os.environ.get("AIRFLOW_URL", "http://localhost:8080/api/v1")
        airflow_user = os.environ.get("AIRFLOW_USER", "admin")
        airflow_password = os.environ["AIRFLOW_PASSWORD"]

        # Use a unique suffix so repeated runs don't pollute the shared ./dags/
        suffix = uuid.uuid4().hex[:6]
        team_a = f"tenant_a_{suffix}"
        team_b = f"tenant_b_{suffix}"

        adp_a = self._make_tenant_adapter(dags_folder, airflow_url, airflow_user, airflow_password, team_a)
        adp_b = self._make_tenant_adapter(dags_folder, airflow_url, airflow_user, airflow_password, team_b)

        job_ids_to_cleanup: list[tuple[AirflowSchedulerAdapter, str]] = []
        try:
            # Same *logical* job_name from both tenants must land in separate
            # DAG subdirectories with distinct dag_ids.
            job_name = "daily_report"
            payload = SchedulerJobPayload(
                job_name=job_name,
                sql="SELECT 1 AS value",
                db_connection={"url": "sqlite:///:memory:"},
                schedule=None,
                description="multi-tenant integration test",
            )

            job_a = adp_a.submit_job(payload)
            job_ids_to_cleanup.append((adp_a, job_a.job_id))

            job_b = adp_b.submit_job(payload)
            job_ids_to_cleanup.append((adp_b, job_b.job_id))

            # dag_ids must differ and carry the tenant prefix
            assert job_a.job_id == f"{team_a}__{job_name}"
            assert job_b.job_id == f"{team_b}__{job_name}"
            assert job_a.job_id != job_b.job_id

            # Files landed under distinct subdirectories
            assert (dags_folder / team_a / f"{job_a.job_id}.py").exists()
            assert (dags_folder / team_b / f"{job_b.job_id}.py").exists()

            # Each tenant's list_jobs returns only their own DAGs
            a_ids = {j.job_id for j in adp_a.list_jobs().items}
            b_ids = {j.job_id for j in adp_b.list_jobs().items}
            assert job_a.job_id in a_ids
            assert job_a.job_id not in b_ids
            assert job_b.job_id in b_ids
            assert job_b.job_id not in a_ids

            # No cross-leak: every DAG that A sees must bear A's prefix, and same for B
            assert all(d.startswith(f"{team_a}__") for d in a_ids)
            assert all(d.startswith(f"{team_b}__") for d in b_ids)

            # Conflict detection is per-tenant: submitting the same job_name
            # again from the SAME adapter must raise, but the OTHER adapter's
            # identical job_name has already succeeded above.
            with pytest.raises(SchedulerJobConflictError):
                adp_a.submit_job(payload)
        finally:
            for adp, dag_id in job_ids_to_cleanup:
                try:
                    adp.delete_job(dag_id)
                except Exception:
                    pass
            adp_a.close()
            adp_b.close()
