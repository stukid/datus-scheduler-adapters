# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Unit tests for datus-airflow adapter (no live Airflow required)."""

import sys
from pathlib import Path
from types import SimpleNamespace
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
            dag_id="d",
            job_name="d",
            sql="SELECT 1",
            db_connection=None,
            schedule=None,
            start_date=None,
            end_date=None,
            description=None,
        )
        assert "schedule=None" in source

    def test_string_none_schedule_renders_none(self) -> None:
        """LLM may pass the string 'None' instead of Python None; must still render schedule=None."""
        source = render_dag_source(
            dag_id="d",
            job_name="d",
            sql="SELECT 1",
            db_connection=None,
            schedule="None",
            start_date=None,
            end_date=None,
            description=None,
        )
        assert "schedule=None" in source
        assert "schedule='None'" not in source

    def test_cron_schedule_quoted(self) -> None:
        source = render_dag_source(
            dag_id="d",
            job_name="d",
            sql="SELECT 1",
            db_connection=None,
            schedule="0 6 * * 1",
            start_date=None,
            end_date=None,
            description=None,
        )
        assert "'0 6 * * 1'" in source or '"0 6 * * 1"' in source

    def test_no_db_connection_renders_empty_dict(self) -> None:
        source = render_dag_source(
            dag_id="d",
            job_name="d",
            sql="SELECT 1",
            db_connection=None,
            schedule=None,
            start_date=None,
            end_date=None,
            description=None,
        )
        assert '"db_connection": {}' in source


# ── AirflowSchedulerAdapter unit tests ───────────────────────────────────


class TestDagId:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("Daily Report", "daily_report"),
            ("my-job-name", "my_job_name"),
            ("UPPER CASE", "upper_case"),
            ("already_valid", "already_valid"),
        ],
    )
    def test_to_dag_id(self, adapter: AirflowSchedulerAdapter, name: str, expected: str) -> None:
        # The fixture uses legacy dags_folder-only config where dag_id_prefix
        # defaults to "" for backward compatibility, so the raw sanitized
        # base IS the full dag_id.
        assert adapter._to_dag_id(name) == expected


class TestMapRunStatus:
    @pytest.mark.parametrize(
        "state,expected",
        [
            ("success", RunStatus.SUCCESS),
            ("failed", RunStatus.FAILED),
            ("running", RunStatus.RUNNING),
            ("queued", RunStatus.PENDING),
            (None, RunStatus.UNKNOWN),
            ("unknown_state", RunStatus.UNKNOWN),
        ],
    )
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

    def test_to_dag_id_rejects_path_traversal(self, adapter: AirflowSchedulerAdapter) -> None:
        """dag_id with path traversal characters must be rejected."""
        from datus_scheduler_core.exceptions import SchedulerException as _SE

        with pytest.raises(_SE, match="unsafe characters"):
            adapter._to_dag_id("../etc/passwd")

        with pytest.raises(_SE, match="unsafe characters"):
            adapter._to_dag_id("dag/with/slash")


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


class TestDuckDBDagTemplate:
    """Tests for DuckDB connection handling in the rendered DAG template."""

    def test_rendered_source_contains_duckdb_branch(self) -> None:
        source = render_dag_source(
            dag_id="duckdb_dag",
            job_name="DuckDB Job",
            sql="SELECT 1",
            db_connection={"conn_id": "my_duckdb"},
            schedule=None,
            start_date=None,
            end_date=None,
            description=None,
        )
        assert 'conn.conn_type == "duckdb"' in source
        assert "extra_dejson" in source

    def test_duckdb_conn_id_embedded(self) -> None:
        source = render_dag_source(
            dag_id="d",
            job_name="d",
            sql="SELECT 1",
            db_connection={"conn_id": "duckdb_dacomp_lever"},
            schedule=None,
            start_date=None,
            end_date=None,
            description=None,
        )
        assert "duckdb_dacomp_lever" in source

    @staticmethod
    def _call_resolve_url(conn_attrs: dict) -> str:
        """Render DAG, exec with mocked Airflow, and call _resolve_connection_url."""
        source = render_dag_source(
            dag_id="test",
            job_name="test",
            sql="SELECT 1",
            db_connection={"conn_id": "test_conn"},
            schedule=None,
            start_date=None,
            end_date=None,
            description=None,
        )
        conn_obj = SimpleNamespace(**conn_attrs)
        mock_base_hook = MagicMock()
        mock_base_hook.get_connection.return_value = conn_obj

        airflow_mocks = {
            "airflow": MagicMock(),
            "airflow.hooks": MagicMock(),
            "airflow.hooks.base": MagicMock(BaseHook=mock_base_hook),
            "airflow.operators": MagicMock(),
            "airflow.operators.python": MagicMock(),
        }
        ns: dict = {}
        with patch.dict(sys.modules, airflow_mocks):
            exec(compile(source, "<test>", "exec"), ns)  # noqa: S102
            return ns["_resolve_connection_url"]({"conn_id": "test_conn"})

    def test_duckdb_absolute_path(self) -> None:
        url = self._call_resolve_url(
            {
                "conn_type": "duckdb",
                "schema": "",
                "host": "/opt/airflow/data/foo.duckdb",
                "extra_dejson": {},
            }
        )
        assert url == "duckdb:////opt/airflow/data/foo.duckdb"

    def test_duckdb_relative_path(self) -> None:
        url = self._call_resolve_url(
            {
                "conn_type": "duckdb",
                "schema": "local.db",
                "host": "",
                "extra_dejson": {},
            }
        )
        assert url == "duckdb:///local.db"

    def test_duckdb_memory(self) -> None:
        url = self._call_resolve_url(
            {
                "conn_type": "duckdb",
                "schema": ":memory:",
                "host": "",
                "extra_dejson": {},
            }
        )
        assert url == "duckdb:///:memory:"

    def test_duckdb_query_params_preserved(self) -> None:
        url = self._call_resolve_url(
            {
                "conn_type": "duckdb",
                "schema": "",
                "host": "/opt/airflow/data/foo.duckdb",
                "extra_dejson": {"read_only": "true"},
            }
        )
        assert url == "duckdb:////opt/airflow/data/foo.duckdb?read_only=true"

    def test_duckdb_multiple_query_params(self) -> None:
        url = self._call_resolve_url(
            {
                "conn_type": "duckdb",
                "schema": "",
                "host": "/opt/airflow/data/foo.duckdb",
                "extra_dejson": {"read_only": "true", "threads": "4"},
            }
        )
        assert "duckdb:////opt/airflow/data/foo.duckdb?" in url
        assert "read_only=true" in url
        assert "threads=4" in url

    def test_duckdb_no_query_params_no_question_mark(self) -> None:
        url = self._call_resolve_url(
            {
                "conn_type": "duckdb",
                "schema": "",
                "host": "/opt/airflow/data/foo.duckdb",
                "extra_dejson": {},
            }
        )
        assert "?" not in url

    def test_duckdb_schema_takes_priority_over_host(self) -> None:
        url = self._call_resolve_url(
            {
                "conn_type": "duckdb",
                "schema": "/data/primary.duckdb",
                "host": "/data/secondary.duckdb",
                "extra_dejson": {},
            }
        )
        assert url == "duckdb:////data/primary.duckdb"

    def test_duckdb_empty_path_raises(self) -> None:
        """Empty schema and host should raise, not produce ambiguous duckdb:///."""
        with pytest.raises(ValueError, match="no database path"):
            self._call_resolve_url(
                {
                    "conn_type": "duckdb",
                    "schema": "",
                    "host": "",
                    "extra_dejson": {},
                }
            )

    def test_duckdb_query_params_special_chars_encoded(self) -> None:
        """Reserved characters in extras must be percent-encoded."""
        url = self._call_resolve_url(
            {
                "conn_type": "duckdb",
                "schema": "",
                "host": "/data/foo.duckdb",
                "extra_dejson": {"path": "/tmp/a b", "filter": "x=1&y=2"},
            }
        )
        assert "duckdb:////data/foo.duckdb?" in url
        # Values must be percent-encoded, not raw
        assert "a b" not in url  # space must be encoded
        assert "x=1&y=2" not in url  # raw & must be encoded
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        assert params["path"] == ["/tmp/a b"]
        assert params["filter"] == ["x=1&y=2"]

    @staticmethod
    def _call_resolve_url_with_conn(conn_obj) -> str:
        """Like _call_resolve_url but accepts a pre-built conn object."""
        source = render_dag_source(
            dag_id="test",
            job_name="test",
            sql="SELECT 1",
            db_connection={"conn_id": "test_conn"},
            schedule=None,
            start_date=None,
            end_date=None,
            description=None,
        )
        mock_base_hook = MagicMock()
        mock_base_hook.get_connection.return_value = conn_obj

        airflow_mocks = {
            "airflow": MagicMock(),
            "airflow.hooks": MagicMock(),
            "airflow.hooks.base": MagicMock(BaseHook=mock_base_hook),
            "airflow.operators": MagicMock(),
            "airflow.operators.python": MagicMock(),
        }
        ns: dict = {}
        with patch.dict(sys.modules, airflow_mocks):
            exec(compile(source, "<test>", "exec"), ns)  # noqa: S102
            return ns["_resolve_connection_url"]({"conn_id": "test_conn"})

    def test_postgres_connection_normalized(self) -> None:
        """Non-DuckDB path: postgres:// must be normalized to postgresql://."""
        conn = MagicMock()
        conn.conn_type = "postgres"
        conn.get_uri.return_value = "postgres://user:pass@host:5432/mydb"
        url = self._call_resolve_url_with_conn(conn)
        assert url == "postgresql://user:pass@host:5432/mydb"

    def test_mysql_connection_passthrough(self) -> None:
        """Non-DuckDB, non-postgres connections pass through unchanged."""
        conn = MagicMock()
        conn.conn_type = "mysql"
        conn.get_uri.return_value = "mysql://user:pass@host:3306/mydb"
        url = self._call_resolve_url_with_conn(conn)
        assert url == "mysql://user:pass@host:3306/mydb"


# ── Multi-tenant DAG folder tests ─────────────────────────────────────────


class TestAirflowConfigMultiTenant:
    """AirflowConfig validator behaviour for multi-tenant deployments."""

    def _base_kwargs(self, **overrides: object) -> dict:
        base = dict(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
        )
        base.update(overrides)
        return base

    def test_resolves_from_root_and_project_name(self, tmp_path: Path) -> None:
        cfg = AirflowConfig(
            **self._base_kwargs(
                dags_folder_root=str(tmp_path),
                project_name="reports-team",
            )
        )
        assert cfg.dags_folder == str(tmp_path / "reports-team")
        # Hyphens sanitized in the prefix (dag_id must be [a-z0-9_])
        assert cfg.dag_id_prefix == "reports_team__"

    def test_env_var_fallback_for_root(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATUS_AIRFLOW_DAGS_ROOT", str(tmp_path))
        cfg = AirflowConfig(**self._base_kwargs(project_name="team-b"))
        assert cfg.dags_folder == str(tmp_path / "team-b")
        assert cfg.dag_id_prefix == "team_b__"

    def test_mutual_exclusion_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="set either 'dags_folder'"):
            AirflowConfig(
                **self._base_kwargs(
                    dags_folder=str(tmp_path),
                    dags_folder_root=str(tmp_path / "other"),
                    project_name="x",
                )
            )

    def test_missing_project_name_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="project_name is required"):
            AirflowConfig(**self._base_kwargs(dags_folder_root=str(tmp_path)))

    def test_missing_all_paths_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATUS_AIRFLOW_DAGS_ROOT", raising=False)
        with pytest.raises(ValueError, match="requires one of"):
            AirflowConfig(**self._base_kwargs(project_name="x"))

    def test_legacy_dags_folder_still_works(self, tmp_path: Path) -> None:
        """Backward-compat: old configs with only dags_folder keep working."""
        cfg = AirflowConfig(**self._base_kwargs(dags_folder=str(tmp_path)))
        assert cfg.dags_folder == str(tmp_path)
        # With no project_name, prefix defaults to "" to preserve prior behavior
        assert cfg.dag_id_prefix == ""

    def test_explicit_empty_prefix_preserved(self, tmp_path: Path) -> None:
        """Explicit '' means 'disable prefixing', not 'auto-derive'."""
        cfg = AirflowConfig(
            **self._base_kwargs(
                dags_folder_root=str(tmp_path),
                project_name="team-a",
                dag_id_prefix="",
            )
        )
        assert cfg.dag_id_prefix == ""

    def test_explicit_custom_prefix_preserved(self, tmp_path: Path) -> None:
        cfg = AirflowConfig(
            **self._base_kwargs(
                dags_folder_root=str(tmp_path),
                project_name="team-a",
                dag_id_prefix="custom__",
            )
        )
        assert cfg.dag_id_prefix == "custom__"

    def test_invalid_project_name_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="project_name"):
            AirflowConfig(
                **self._base_kwargs(
                    dags_folder_root=str(tmp_path),
                    project_name="bad/name",  # slash is not allowed
                )
            )

    @pytest.mark.parametrize("bad", [".", "..", "...", "..foo", ".hidden", "foo..bar"])
    def test_project_name_rejects_path_traversal(self, tmp_path: Path, bad: str) -> None:
        """project_name is joined to dags_folder_root via Path; '.', '..', and
        any '..' substring could escape the intended tenant directory."""
        with pytest.raises(ValueError, match="project_name"):
            AirflowConfig(
                **self._base_kwargs(
                    dags_folder_root=str(tmp_path),
                    project_name=bad,
                )
            )

    def test_invalid_dag_id_prefix_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="dag_id_prefix"):
            AirflowConfig(
                **self._base_kwargs(
                    dags_folder=str(tmp_path),
                    dag_id_prefix="Has-Caps-And-Hyphens",
                )
            )

    @pytest.mark.parametrize("bad", ["team", "team_", "a", "_"])
    def test_dag_id_prefix_must_end_with_double_underscore(self, tmp_path: Path, bad: str) -> None:
        """A prefix that does not end with exactly ``__`` allows
        startswith() leakage across tenants (e.g. prefix ``team`` matches
        ``team_work``). The validator must enforce the ``__`` separator
        convention for non-empty prefixes."""
        with pytest.raises(ValueError, match="dag_id_prefix"):
            AirflowConfig(
                **self._base_kwargs(
                    dags_folder=str(tmp_path),
                    dag_id_prefix=bad,
                )
            )

    def test_dag_id_prefix_rejects_trailing_newline(self, tmp_path: Path) -> None:
        """re.match() with ``$`` anchors accepts a trailing newline; we use
        fullmatch() to close that hole so log-injection-shaped inputs don't
        sneak through validation."""
        with pytest.raises(ValueError, match="dag_id_prefix"):
            AirflowConfig(
                **self._base_kwargs(
                    dags_folder=str(tmp_path),
                    dag_id_prefix="team__\n",
                )
            )

    def test_project_name_rejects_trailing_newline(self, tmp_path: Path) -> None:
        """Same fullmatch fix as dag_id_prefix, applied to project_name."""
        with pytest.raises(ValueError, match="project_name"):
            AirflowConfig(
                **self._base_kwargs(
                    dags_folder_root=str(tmp_path),
                    project_name="team-a\n",
                )
            )

    def test_empty_dag_id_prefix_accepted(self, tmp_path: Path) -> None:
        """Explicit empty string disables prefixing (legacy / single-tenant)."""
        cfg = AirflowConfig(
            **self._base_kwargs(
                dags_folder=str(tmp_path),
                dag_id_prefix="",
            )
        )
        assert cfg.dag_id_prefix == ""


class TestAdapterMultiTenant:
    """Adapter behaviour in multi-tenant mode."""

    def _make_adapter(self, config: AirflowConfig) -> AirflowSchedulerAdapter:
        with patch("datus_scheduler_airflow.adapter.httpx.Client") as mock_client_cls:
            mock_client_cls.return_value = MagicMock()
            return AirflowSchedulerAdapter(config)

    def test_init_creates_subdirectory(self, tmp_path: Path) -> None:
        root = tmp_path / "dags"
        assert not root.exists()
        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder_root=str(root),
            project_name="team-a",
        )
        self._make_adapter(cfg)
        # {root}/team-a should have been created by _ensure_dags_folder
        assert (root / "team-a").is_dir()

    def test_init_fails_when_path_not_writable(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """mkdir succeeds but the actual write probe fails -> fail fast.

        Simulates a read-only NFS / JuiceFS mount where directory creation
        may succeed (via server permissions) but creating a file inside
        raises PermissionError. We target the probe's write_text call rather
        than os.access because os.access is advisory on networked filesystems
        and the adapter no longer relies on it.
        """
        from datus_scheduler_core.exceptions import SchedulerConnectionError

        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder=str(tmp_path),
        )

        real_write_text = Path.write_text

        def fake_write_text(self: Path, *args: object, **kwargs: object) -> int:
            # Intercept only the probe, leave everything else alone.
            if self.name.startswith(".datus_write_probe_"):
                raise PermissionError("simulated read-only mount")
            return real_write_text(self, *args, **kwargs)

        monkeypatch.setattr("pathlib.Path.write_text", fake_write_text)
        with pytest.raises(SchedulerConnectionError, match="not writable"):
            self._make_adapter(cfg)

    def test_to_dag_id_applies_prefix(self, tmp_path: Path) -> None:
        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder_root=str(tmp_path),
            project_name="team-a",
        )
        adp = self._make_adapter(cfg)
        assert adp._to_dag_id("daily sales") == "team_a__daily_sales"
        assert adp._to_dag_id("Weekly Report") == "team_a__weekly_report"

    def test_list_jobs_filters_by_prefix(self, tmp_path: Path) -> None:
        """list_jobs must only return DAGs owned by this instance."""
        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder_root=str(tmp_path),
            project_name="team-a",
        )
        adp = self._make_adapter(cfg)

        # Mock Airflow /dags response with DAGs from two tenants
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "dags": [
                {"dag_id": "team_a__foo", "is_paused": False},
                {"dag_id": "team_a__bar", "is_paused": False},
                {"dag_id": "team_b__foo", "is_paused": False},  # other tenant
                {"dag_id": "unrelated_legacy_dag", "is_paused": True},
            ]
        }
        adp._session.get.return_value = resp

        jobs = adp.list_jobs()

        # Only the two team_a DAGs should come back
        assert len(jobs) == 2
        returned_ids = {j.job_id for j in jobs}
        assert returned_ids == {"team_a__foo", "team_a__bar"}

        # dag_id_pattern is NOT sent to the server because Airflow interprets
        # it as a SQL LIKE expression (underscores are wildcards), which
        # would leak other tenants' DAGs whose names happen to match the
        # resulting pattern. All filtering happens client-side.
        call_kwargs = adp._session.get.call_args.kwargs
        assert "dag_id_pattern" not in call_kwargs["params"]

    def test_list_jobs_does_not_leak_on_sql_like_wildcard(self, tmp_path: Path) -> None:
        """Regression: a DAG whose id matches the prefix's SQL LIKE expansion
        (where ``_`` is a single-char wildcard) but not the intended literal
        prefix must NOT be returned. With the old server-side filter the
        server would happily return ``teamXaXXfoo`` for pattern ``team_a__``.
        """
        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder_root=str(tmp_path),
            project_name="team-a",
        )
        adp = self._make_adapter(cfg)

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "dags": [
                {"dag_id": "team_a__legit", "is_paused": False},
                # These all match "team_a__" as a SQL LIKE pattern because _ is a wildcard:
                {"dag_id": "teamXaXXfoo", "is_paused": False},
                {"dag_id": "team1a23bar", "is_paused": False},
            ]
        }
        adp._session.get.return_value = resp

        jobs = adp.list_jobs()

        assert {j.job_id for j in jobs} == {"team_a__legit"}

    def test_list_jobs_no_prefix_filter_in_legacy_mode(self, tmp_path: Path) -> None:
        """Legacy dags_folder-only config should not filter list_jobs."""
        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder=str(tmp_path),
        )
        adp = self._make_adapter(cfg)

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "dags": [
                {"dag_id": "any_dag", "is_paused": False},
                {"dag_id": "another", "is_paused": False},
            ]
        }
        adp._session.get.return_value = resp

        jobs = adp.list_jobs()
        assert len(jobs) == 2

        # No dag_id_pattern sent to Airflow
        call_kwargs = adp._session.get.call_args.kwargs
        assert "dag_id_pattern" not in call_kwargs["params"]

    def test_list_jobs_applies_status_filter_before_slice(self, tmp_path: Path) -> None:
        """Regression: status filter must apply BEFORE offset/limit slicing,
        otherwise a page containing mostly paused DAGs can return far fewer
        than ``limit`` active DAGs even when plenty of active ones exist."""
        from datus_scheduler_core.models import JobStatus

        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder_root=str(tmp_path),
            project_name="team-a",
        )
        adp = self._make_adapter(cfg)

        # 5 active + 5 paused, interleaved. Request 3 active.
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "dags": [
                {"dag_id": "team_a__active_1", "is_paused": False},
                {"dag_id": "team_a__paused_1", "is_paused": True},
                {"dag_id": "team_a__active_2", "is_paused": False},
                {"dag_id": "team_a__paused_2", "is_paused": True},
                {"dag_id": "team_a__active_3", "is_paused": False},
                {"dag_id": "team_a__paused_3", "is_paused": True},
                {"dag_id": "team_a__active_4", "is_paused": False},
                {"dag_id": "team_a__paused_4", "is_paused": True},
                {"dag_id": "team_a__active_5", "is_paused": False},
                {"dag_id": "team_a__paused_5", "is_paused": True},
            ]
        }
        adp._session.get.return_value = resp

        jobs = adp.list_jobs(status=JobStatus.ACTIVE, limit=3)
        assert len(jobs) == 3
        assert all(j.status == JobStatus.ACTIVE for j in jobs)
        assert [j.job_id for j in jobs] == [
            "team_a__active_1",
            "team_a__active_2",
            "team_a__active_3",
        ]

    @pytest.mark.parametrize(
        "method_name, extra_args",
        [
            ("trigger_job", ()),
            ("pause_job", ()),
            ("resume_job", ()),
            ("delete_job", ()),
            ("list_job_runs", ()),
            ("get_run_log", ("run_xyz",)),
        ],
    )
    def test_tenant_guard_rejects_foreign_dag_id(
        self, tmp_path: Path, method_name: str, extra_args: tuple
    ) -> None:
        """Exact-id operations must refuse to act on another tenant's DAG.

        Without this guard a caller that knows (or guesses) another tenant's
        dag_id could trigger, pause, or delete it, bypassing the prefix
        isolation applied to list_jobs.
        """
        from datus_scheduler_core.exceptions import SchedulerJobNotFoundError

        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder_root=str(tmp_path),
            project_name="team-a",
        )
        adp = self._make_adapter(cfg)

        method = getattr(adp, method_name)
        with pytest.raises(SchedulerJobNotFoundError):
            method("team_b__foreign_dag", *extra_args)

        # Guard short-circuits before any HTTP traffic.
        adp._session.post.assert_not_called()
        adp._session.patch.assert_not_called()
        adp._session.delete.assert_not_called()
        adp._session.get.assert_not_called()

    def test_tenant_guard_get_job_returns_none_for_foreign(self, tmp_path: Path) -> None:
        """get_job preserves its Optional[...] contract: foreign ids are
        reported as missing rather than raising, so callers can use it as an
        existence probe."""
        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder_root=str(tmp_path),
            project_name="team-a",
        )
        adp = self._make_adapter(cfg)

        assert adp.get_job("team_b__foreign_dag") is None
        adp._session.get.assert_not_called()

    def test_tenant_guard_get_job_run_returns_none_for_foreign(self, tmp_path: Path) -> None:
        cfg = AirflowConfig(
            name="t",
            type="airflow",
            api_base_url="http://localhost:8080/api/v1",
            username="admin",
            password="admin",
            dags_folder_root=str(tmp_path),
            project_name="team-a",
        )
        adp = self._make_adapter(cfg)

        assert adp.get_job_run("team_b__foreign_dag", "run_xyz") is None
        adp._session.get.assert_not_called()
