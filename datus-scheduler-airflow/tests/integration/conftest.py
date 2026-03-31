# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Pytest configuration for Airflow integration tests.

How to run
----------
1. Start Airflow:
       cd datus-scheduler-adapters/datus-airflow
       docker compose up -d
       # wait ~60s for Airflow to initialise

2. Run integration tests:
       cd datus-scheduler-adapters
       uv run pytest datus-airflow/tests/integration/ -v -m integration

Environment variables (override defaults if needed):
    AIRFLOW_URL        Base URL of the REST API  (default: http://localhost:8080/api/v1)
    AIRFLOW_USER       Username                  (default: admin)
    AIRFLOW_PASSWORD   Password                  (default: admin123)
    AIRFLOW_DAGS_DIR   Local dags directory      (default: ./dags relative to this file)
"""

import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

from datus_scheduler_airflow.adapter import AirflowSchedulerAdapter
from datus_scheduler_core.config import AirflowConfig

# ── Configuration from environment ────────────────────────────────────────

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080/api/v1")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD")
if not AIRFLOW_PASSWORD:
    pytest.skip("AIRFLOW_PASSWORD env var not set", allow_module_level=True)

# Default dags dir: the ./dags folder next to docker-compose.yml
_THIS_DIR = Path(__file__).parent.parent.parent  # datus-airflow/
AIRFLOW_DAGS_DIR = os.environ.get("AIRFLOW_DAGS_DIR", str(_THIS_DIR / "dags"))

# ── Helpers ────────────────────────────────────────────────────────────────


def _wait_for_airflow(max_wait: int = 120, poll: int = 5) -> bool:
    """Poll the Airflow health endpoint until it responds 200 or timeout."""
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(
                f"{AIRFLOW_URL}/health",
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                timeout=5,
            )
            if resp.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(poll)
    return False


# ── Session-scoped fixtures ────────────────────────────────────────────────


@pytest.fixture(scope="session")
def airflow_ready() -> bool:
    """Wait for Airflow to be healthy.  Skip the session if it never comes up."""
    ready = _wait_for_airflow()
    if not ready:
        pytest.skip(
            "Airflow is not available at "
            f"{AIRFLOW_URL}. "
            "Run: docker compose up -d  (from datus-airflow/)"
        )
    return True


@pytest.fixture(scope="session")
def dags_folder() -> Path:
    p = Path(AIRFLOW_DAGS_DIR)
    p.mkdir(parents=True, exist_ok=True)
    return p


@pytest.fixture(scope="session")
def airflow_config(dags_folder: Path) -> AirflowConfig:
    return AirflowConfig(
        name="airflow_test",
        type="airflow",
        api_base_url=AIRFLOW_URL,
        username=AIRFLOW_USER,
        password=AIRFLOW_PASSWORD,
        dags_folder=str(dags_folder),
        dag_discovery_timeout=90,
        dag_discovery_poll_interval=5,
    )


@pytest.fixture(scope="session")
def adapter(airflow_config: AirflowConfig, airflow_ready: bool) -> AirflowSchedulerAdapter:
    adp = AirflowSchedulerAdapter(airflow_config)
    yield adp
    adp.close()


# ── Test-scoped DAG cleanup ────────────────────────────────────────────────


@pytest.fixture()
def cleanup_dag(adapter: AirflowSchedulerAdapter):
    """Collect dag_ids during a test and delete them after the test completes."""
    dag_ids: list[str] = []
    yield dag_ids
    for dag_id in dag_ids:
        try:
            adapter.delete_job(dag_id)
        except Exception:
            pass
