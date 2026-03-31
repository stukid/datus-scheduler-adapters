# datus-scheduler-airflow

Apache Airflow scheduler adapter for Datus.

## Install

```bash
pip install datus-scheduler-airflow
```

## Quick Start

```python
from datus_airflow import AirflowSchedulerAdapter
from datus_scheduler_core.config import AirflowConfig
from datus_scheduler_core.models import SchedulerJobPayload

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

job = adapter.submit_job(SchedulerJobPayload(
    job_name="daily_report",
    sql="SELECT count(*) FROM orders",
    db_connection={"url": "postgresql://user:pass@host/db"},
    schedule="0 8 * * *",
))
print(job.job_id)
```

## Integration Tests

```bash
# Start Airflow
docker compose up -d

# Run tests
uv run pytest tests/integration/ -v -m integration
```
