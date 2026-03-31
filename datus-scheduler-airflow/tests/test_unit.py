# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Unit tests for datus-airflow adapter (no live Airflow required)."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from datus_scheduler_airflow.adapter import AirflowSchedulerAdapter, _map_run_status
from datus_scheduler_airflow.dag_template import render_dag_source, render_spark_dag_source, render_sparksql_dag_source
from datus_scheduler_core.config import AirflowConfig
from datus_scheduler_core.exceptions import (
    SchedulerJobConflictError,
    SchedulerJobNotFoundError,
)
from datus_scheduler_core.models import JobStatus, RunStatus, SchedulerJobPayload


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture()
def airflow_config(tmp_path: Path) -> AirflowConfig:
    return AirflowConfig(
        name="airflow_test",
        type="airflow",
        api_base_url="http://localhost:8080/api/v1",
        username="admin",
        password="admin",
        dags_folder=str(tmp_path),
        dag_discovery_timeout=5,
        dag_discovery_poll_interval=1,
    )


@pytest.fixture()
def adapter(airflow_config: AirflowConfig) -> AirflowSchedulerAdapter:
    with patch("datus_scheduler_airflow.adapter.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value = MagicMock()
        adp = AirflowSchedulerAdapter(airflow_config)
    return adp


@pytest.fixture()
def sample_payload() -> SchedulerJobPayload:
    return SchedulerJobPayload(
        job_name="daily_report",
        sql="SELECT 1 AS value",
        db_type="sqlite",
        db_connection={"url": "sqlite:///:memory:"},
        schedule="0 8 * * *",
        description="Test job",
    )


# ── dag_template tests ────────────────────────────────────────────────────


class TestDagTemplate:
    def test_renders_valid_python(self) -> None:
        source = render_dag_source(
            dag_id="test_dag",
            job_name="Test Job",
            sql="SELECT 1",
            db_connection={"url": "sqlite:///:memory:"},
            schedule="@daily",
            start_date=None,
            end_date=None,
            description="desc",
        )
        # Must be compilable Python
        compile(source, "<generated>", "exec")

    def test_dag_id_in_source(self) -> None:
        source = render_dag_source(
            dag_id="my_dag_id",
            job_name="My Job",
            sql="SELECT 2",
            db_connection=None,
            schedule=None,
            start_date=None,
            end_date=None,
            description=None,
        )
        assert "'my_dag_id'" in source or '"my_dag_id"' in source

    def test_sql_embedded_in_config_json(self) -> None:
        sql = "SELECT id, name FROM users WHERE active = 1"
        source = render_dag_source(
            dag_id="users_dag",
            job_name="Users",
            sql=sql,
            db_connection={"url": "postgresql://host/db"},
            schedule=None,
            start_date=None,
            end_date=None,
            description=None,
        )
        assert sql in source

    def test_none_schedule_renders_none(self) -> None:
        source = render_dag_source(
            dag_id="d", job_name="d", sql="SELECT 1",
            db_connection=None, schedule=None, start_date=None,
            end_date=None, description=None,
        )
        assert "schedule=None" in source

    def test_string_none_schedule_renders_none(self) -> None:
        """LLM may pass the string 'None' instead of Python None; must still render schedule=None."""
        source = render_dag_source(
            dag_id="d", job_name="d", sql="SELECT 1",
            db_connection=None, schedule="None", start_date=None,
            end_date=None, description=None,
        )
        assert "schedule=None" in source
        assert "schedule='None'" not in source

    def test_cron_schedule_quoted(self) -> None:
        source = render_dag_source(
            dag_id="d", job_name="d", sql="SELECT 1",
            db_connection=None, schedule="0 6 * * 1",
            start_date=None, end_date=None, description=None,
        )
        assert "'0 6 * * 1'" in source or '"0 6 * * 1"' in source

    def test_no_db_connection_renders_empty_dict(self) -> None:
        source = render_dag_source(
            dag_id="d", job_name="d", sql="SELECT 1",
            db_connection=None, schedule=None, start_date=None,
            end_date=None, description=None,
        )
        assert '"db_connection": {}' in source


# ── AirflowSchedulerAdapter unit tests ───────────────────────────────────


class TestDagId:
    @pytest.mark.parametrize("name,expected", [
        ("Daily Report", "daily_report"),
        ("my-job-name", "my_job_name"),
        ("UPPER CASE", "upper_case"),
        ("already_valid", "already_valid"),
    ])
    def test_to_dag_id(self, name: str, expected: str) -> None:
        assert AirflowSchedulerAdapter._to_dag_id(name) == expected


class TestMapRunStatus:
    @pytest.mark.parametrize("state,expected", [
        ("success", RunStatus.SUCCESS),
        ("failed", RunStatus.FAILED),
        ("running", RunStatus.RUNNING),
        ("queued", RunStatus.PENDING),
        (None, RunStatus.UNKNOWN),
        ("unknown_state", RunStatus.UNKNOWN),
    ])
    def test_mapping(self, state: str, expected: RunStatus) -> None:
        assert _map_run_status(state) == expected


class TestRegister:
    def test_register_adds_to_registry(self) -> None:
        from datus_scheduler_core.registry import SchedulerAdapterRegistry
        SchedulerAdapterRegistry.reset()
        import datus_scheduler_airflow
        datus_scheduler_airflow.register()
        assert SchedulerAdapterRegistry.is_registered("airflow")
        meta = SchedulerAdapterRegistry.get_metadata("airflow")
        assert meta is not None
        assert meta.display_name == "Apache Airflow"
        assert meta.config_class is not None


class TestDagFileOperations:
    def test_dag_file_path(self, adapter: AirflowSchedulerAdapter, tmp_path: Path) -> None:
        adapter._config.dags_folder = str(tmp_path)
        expected = tmp_path / "my_dag.py"
        assert adapter._dag_file_path("my_dag") == expected

    def test_write_dag_file_creates_file(self, adapter: AirflowSchedulerAdapter, tmp_path: Path) -> None:
        adapter._config.dags_folder = str(tmp_path)
        adapter._write_dag_file("test_dag", "# content")
        assert (tmp_path / "test_dag.py").read_text() == "# content"

    def test_remove_dag_file_deletes_file(self, adapter: AirflowSchedulerAdapter, tmp_path: Path) -> None:
        adapter._config.dags_folder = str(tmp_path)
        p = tmp_path / "test_dag.py"
        p.write_text("# content")
        adapter._remove_dag_file("test_dag")
        assert not p.exists()

    def test_remove_dag_file_is_idempotent(self, adapter: AirflowSchedulerAdapter, tmp_path: Path) -> None:
        adapter._config.dags_folder = str(tmp_path)
        # No file — should not raise
        adapter._remove_dag_file("nonexistent_dag")


class TestSubmitJobMocked:
    """submit_job tests using a fully mocked HTTP session."""

    def _make_adapter(self, tmp_path: Path) -> AirflowSchedulerAdapter:
        config = AirflowConfig(
            name="test",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder=str(tmp_path),
            dag_discovery_timeout=5,
            dag_discovery_poll_interval=1,
        )
        adp = AirflowSchedulerAdapter.__new__(AirflowSchedulerAdapter)
        adp.config = config
        adp._config = config
        adp.api_base_url = config.api_base_url
        adp.timeout = config.timeout_seconds
        adp._session = MagicMock()
        return adp

    def test_submit_job_writes_dag_file(self, tmp_path: Path, sample_payload: SchedulerJobPayload) -> None:
        adp = self._make_adapter(tmp_path)
        # First call (conflict check) returns 404, second call (discovery) returns the dag
        dag_response = MagicMock()
        dag_response.status_code = 200
        dag_response.json.return_value = {
            "dag_id": "daily_report",
            "dag_display_name": "daily_report",
            "is_paused": False,
            "schedule_interval": "0 8 * * *",
        }
        not_found = MagicMock()
        not_found.status_code = 404

        # First call = conflict check (404), subsequent calls = discovery polling
        adp._session.get.side_effect = [not_found] + [dag_response] * 10

        result = adp.submit_job(sample_payload)

        dag_file = tmp_path / "daily_report.py"
        assert dag_file.exists()
        assert "daily_report" in dag_file.read_text()
        assert result.job_id == "daily_report"
        assert result.platform == "airflow"

    def test_submit_job_raises_on_conflict(self, tmp_path: Path, sample_payload: SchedulerJobPayload) -> None:
        adp = self._make_adapter(tmp_path)
        dag_response = MagicMock()
        dag_response.status_code = 200
        dag_response.json.return_value = {
            "dag_id": "daily_report",
            "dag_display_name": "daily_report",
            "is_paused": False,
        }
        adp._session.get.return_value = dag_response

        with pytest.raises(SchedulerJobConflictError):
            adp.submit_job(sample_payload)

    def test_submit_job_requires_sql(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        # get_job must return None (no conflict) so submit_job reaches the sql validation
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        adp._session.get.return_value = mock_resp
        payload = SchedulerJobPayload(job_name="no_sql")
        with pytest.raises(Exception, match="sql"):
            adp.submit_job(payload)

    def test_trigger_job_returns_job_run(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dag_run_id": "manual__2024-01-01T00:00:00+00:00",
            "state": "running",
        }
        adp._session.post.return_value = mock_resp

        run = adp.trigger_job("daily_report")
        assert run.run_id == "manual__2024-01-01T00:00:00+00:00"
        assert run.status == RunStatus.RUNNING

    def test_trigger_job_raises_not_found(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        adp._session.post.return_value = mock_resp

        with pytest.raises(SchedulerJobNotFoundError):
            adp.trigger_job("nonexistent")

    def test_pause_job(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        adp._session.patch.return_value = mock_resp

        adp.pause_job("daily_report")  # Should not raise
        adp._session.patch.assert_called_once_with("/dags/daily_report", json={"is_paused": True})

    def test_resume_job(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        adp._session.patch.return_value = mock_resp

        adp.resume_job("daily_report")
        adp._session.patch.assert_called_once_with("/dags/daily_report", json={"is_paused": False})

    def test_get_job_returns_none_on_404(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        not_found = MagicMock()
        not_found.status_code = 404
        adp._session.get.return_value = not_found

        assert adp.get_job("nonexistent") is None

    def test_get_job_returns_scheduled_job(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dag_id": "daily_report",
            "dag_display_name": "Daily Report",
            "is_paused": True,
            "schedule_interval": "0 8 * * *",
        }
        adp._session.get.return_value = mock_resp

        job = adp.get_job("daily_report")
        assert job is not None
        assert job.job_id == "daily_report"
        assert job.status == JobStatus.PAUSED

    def test_list_jobs(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dags": [
                {"dag_id": "dag1", "is_paused": False},
                {"dag_id": "dag2", "is_paused": True},
            ]
        }
        adp._session.get.return_value = mock_resp

        jobs = adp.list_jobs()
        assert len(jobs) == 2
        assert jobs[0].job_id == "dag1"
        assert jobs[1].status == JobStatus.PAUSED

    def test_delete_job_removes_file_and_calls_api(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)

        # Create the dag file
        dag_file = tmp_path / "my_dag.py"
        dag_file.write_text("# dag content")

        # First get = exists check; subsequent gets for _wait_for_dag_inactive return 404
        get_exists = MagicMock()
        get_exists.status_code = 200
        get_exists.json.return_value = {"dag_id": "my_dag", "is_paused": False, "is_active": True}

        get_inactive = MagicMock()
        get_inactive.status_code = 404

        patch_resp = MagicMock()
        patch_resp.status_code = 200

        delete_resp = MagicMock()
        delete_resp.status_code = 204

        adp._session.get.side_effect = [get_exists, get_inactive]
        adp._session.patch.return_value = patch_resp
        adp._session.delete.return_value = delete_resp

        adp.delete_job("my_dag")

        assert not dag_file.exists()
        adp._session.delete.assert_called_once_with("/dags/my_dag")

    def test_delete_job_raises_not_found(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        not_found = MagicMock()
        not_found.status_code = 404
        adp._session.get.return_value = not_found

        with pytest.raises(SchedulerJobNotFoundError):
            adp.delete_job("nonexistent")

    def test_update_job_overwrites_file(self, tmp_path: Path, sample_payload: SchedulerJobPayload) -> None:
        adp = self._make_adapter(tmp_path)
        # Write an initial DAG file
        dag_file = tmp_path / "daily_report.py"
        dag_file.write_text("# old content")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dag_id": "daily_report",
            "dag_display_name": "daily_report",
            "is_paused": False,
            "schedule_interval": "0 8 * * *",
        }
        adp._session.get.return_value = mock_resp

        result = adp.update_job("daily_report", sample_payload)
        assert result.job_id == "daily_report"
        # File should be overwritten with new content
        assert "# old content" not in dag_file.read_text()
        assert "daily_report" in dag_file.read_text()

    def test_list_job_runs_returns_runs(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dag_runs": [
                {
                    "dag_run_id": "run_1",
                    "state": "success",
                    "start_date": "2024-01-01T00:00:00Z",
                    "end_date": "2024-01-01T01:00:00Z",
                },
                {
                    "dag_run_id": "run_2",
                    "state": "running",
                    "start_date": "2024-01-02T00:00:00Z",
                    "end_date": None,
                },
            ]
        }
        adp._session.get.return_value = mock_resp

        runs = adp.list_job_runs("daily_report")
        assert len(runs) == 2
        assert runs[0].run_id == "run_1"
        assert runs[0].status == RunStatus.SUCCESS
        assert runs[1].status == RunStatus.RUNNING

    def test_list_job_runs_raises_not_found(self, tmp_path: Path) -> None:
        adp = self._make_adapter(tmp_path)
        not_found = MagicMock()
        not_found.status_code = 404
        adp._session.get.return_value = not_found

        with pytest.raises(SchedulerJobNotFoundError):
            adp.list_job_runs("nonexistent")

    def test_to_dag_id_rejects_path_traversal(self) -> None:
        """dag_id with path traversal characters must be rejected."""
        from datus_scheduler_core.exceptions import SchedulerException as _SE

        with pytest.raises(_SE, match="unsafe characters"):
            AirflowSchedulerAdapter._to_dag_id("../etc/passwd")

        with pytest.raises(_SE, match="unsafe characters"):
            AirflowSchedulerAdapter._to_dag_id("dag/with/slash")


class TestSparkDagTemplate:
    def test_renders_valid_python(self) -> None:
        source = render_spark_dag_source(
            dag_id="spark_test",
            job_name="Spark Test",
            spark_script="rdd = sc.parallelize([1,2,3])\nprint(rdd.count())",
        )
        compile(source, "<generated>", "exec")

    def test_dag_id_in_source(self) -> None:
        source = render_spark_dag_source(
            dag_id="my_spark_dag",
            job_name="My Spark",
            spark_script="print('hello')",
        )
        assert "'my_spark_dag'" in source or '"my_spark_dag"' in source

    def test_script_embedded_in_config(self) -> None:
        script = "rdd = sc.parallelize(range(100))"
        source = render_spark_dag_source(
            dag_id="d",
            job_name="d",
            spark_script=script,
        )
        assert script in source

    def test_none_schedule_renders_none(self) -> None:
        source = render_spark_dag_source(
            dag_id="d",
            job_name="d",
            spark_script="pass",
            schedule=None,
        )
        assert "schedule=None" in source

    def test_cron_schedule_quoted(self) -> None:
        source = render_spark_dag_source(
            dag_id="d",
            job_name="d",
            spark_script="pass",
            schedule="0 6 * * 1",
        )
        assert "'0 6 * * 1'" in source or '"0 6 * * 1"' in source

    def test_string_none_schedule_renders_none(self) -> None:
        source = render_spark_dag_source(
            dag_id="d",
            job_name="d",
            spark_script="pass",
            schedule="None",
        )
        assert "schedule=None" in source
        assert "schedule='None'" not in source


class TestSparkSqlDagTemplate:
    def test_renders_valid_python(self) -> None:
        source = render_sparksql_dag_source(
            dag_id="sparksql_test",
            job_name="SparkSQL Test",
            sql="SELECT 1 AS value",
        )
        compile(source, "<generated>", "exec")

    def test_dag_id_in_source(self) -> None:
        source = render_sparksql_dag_source(
            dag_id="my_sparksql_dag",
            job_name="My SparkSQL",
            sql="SELECT 1",
        )
        assert "'my_sparksql_dag'" in source or '"my_sparksql_dag"' in source

    def test_sql_embedded_in_config(self) -> None:
        sql = "SELECT id, name FROM users WHERE active = 1"
        source = render_sparksql_dag_source(
            dag_id="d",
            job_name="d",
            sql=sql,
        )
        assert sql in source

    def test_none_schedule_renders_none(self) -> None:
        source = render_sparksql_dag_source(
            dag_id="d",
            job_name="d",
            sql="SELECT 1",
            schedule=None,
        )
        assert "schedule=None" in source

    def test_cron_schedule_quoted(self) -> None:
        source = render_sparksql_dag_source(
            dag_id="d",
            job_name="d",
            sql="SELECT 1",
            schedule="30 2 * * *",
        )
        assert "'30 2 * * *'" in source or '"30 2 * * *"' in source

    def test_string_none_schedule_renders_none(self) -> None:
        source = render_sparksql_dag_source(
            dag_id="d",
            job_name="d",
            sql="SELECT 1",
            schedule="None",
        )
        assert "schedule=None" in source
        assert "schedule='None'" not in source

    def test_end_date_rendered(self) -> None:
        from datetime import datetime, timezone

        end = datetime(2025, 12, 31, tzinfo=timezone.utc)
        source = render_sparksql_dag_source(
            dag_id="d",
            job_name="d",
            sql="SELECT 1",
            end_date=end,
        )
        assert "2025" in source and "12" in source and "31" in source
