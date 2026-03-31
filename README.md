# datus-scheduler-adapters

Pluggable scheduler adapters for [Datus](https://github.com/Datus-ai/Datus-agent) — manage scheduled SQL and Spark jobs on workflow platforms through a unified Python API.

## Architecture

```
datus-scheduler-core          # Base classes, models, registry, exceptions
├── BaseSchedulerAdapter      # Abstract adapter interface
├── SchedulerJobPayload       # Job submission model
├── ScheduledJob / JobRun     # Status models
└── SchedulerAdapterRegistry  # Plugin discovery

datus-scheduler-airflow       # Apache Airflow adapter
├── AirflowSchedulerAdapter   # Full CRUD implementation
└── dag_template              # DAG source generator (SQL / PySpark / SparkSQL)
```

## Packages

| Package | Description | Install |
|---------|-------------|---------|
| [datus-scheduler-core](datus-scheduler-core/) | Core abstractions and models | `pip install datus-scheduler-core` |
| [datus-scheduler-airflow](datus-scheduler-airflow/) | Apache Airflow adapter | `pip install datus-scheduler-airflow` |

## Adapter capabilities

Each adapter implements the `BaseSchedulerAdapter` interface:

| Operation | Method | Description |
|-----------|--------|-------------|
| Create | `submit_job(payload)` | Register a new scheduled job |
| Update | `update_job(job_id, payload)` | Replace job definition in-place |
| Delete | `delete_job(job_id)` | Remove job and its artifacts |
| Trigger | `trigger_job(job_id)` | Run immediately (ignore schedule) |
| Pause | `pause_job(job_id)` | Suspend automatic scheduling |
| Resume | `resume_job(job_id)` | Resume scheduling |
| Query | `get_job(job_id)` | Get job metadata |
| List | `list_jobs()` | List all jobs |
| Runs | `list_job_runs(job_id)` | Run history |
| Logs | `get_run_log(job_id, run_id)` | Fetch run log text |

## Quick start

```python
from datus_scheduler_airflow.adapter import AirflowSchedulerAdapter
from datus_scheduler_core.config import AirflowConfig
from datus_scheduler_core.models import SchedulerJobPayload

config = AirflowConfig(
    name="my_airflow",
    type="airflow",
    api_base_url="http://localhost:8080/api/v1",
    username="admin",
    password="admin",
    dags_folder="/opt/airflow/dags",
)

adapter = AirflowSchedulerAdapter(config)

# Submit a SQL job with Airflow Connection
job = adapter.submit_job(SchedulerJobPayload(
    job_name="daily_report",
    sql="SELECT count(*) FROM orders WHERE date = CURRENT_DATE",
    db_connection={"conn_id": "starrocks_default"},
    schedule="0 8 * * *",
))

# Trigger, pause, resume
adapter.trigger_job(job.job_id)
adapter.pause_job(job.job_id)
adapter.resume_job(job.job_id)

# Check status
status = adapter.get_job(job.job_id)
runs = adapter.list_job_runs(job.job_id)
```

## Development

```bash
# Clone and install (uv workspace)
git clone https://github.com/Datus-ai/datus-scheduler-adapters.git
cd datus-scheduler-adapters
uv sync

# Run unit tests
uv run pytest datus-scheduler-airflow/tests/test_unit.py -v

# Run integration tests (requires Airflow)
cd datus-scheduler-airflow
docker compose up -d
uv run pytest tests/integration/ -v -m integration
```

## Adding a new adapter

1. Create a new package directory: `datus-scheduler-{platform}/`
2. Implement `BaseSchedulerAdapter` from `datus-scheduler-core`
3. Register via `SchedulerAdapterRegistry.register()` in `__init__.py`
4. Add the package to `[tool.uv.workspace] members` in the root `pyproject.toml`

## License

Apache License 2.0 — see [LICENSE](LICENSE).
