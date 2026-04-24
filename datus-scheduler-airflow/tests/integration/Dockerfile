FROM apache/airflow:2.10.4
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt" \
    "pyspark>=3.4.0" "pymysql>=1.1.0" "psycopg2-binary>=2.9.0"
# DuckDB + StarRocks SQLAlchemy dialects track their own release cadence
# outside the Airflow 2.10.4 constraint set (which pins duckdb==1.1.3 and
# doesn't list starrocks at all), so install them without the constraint.
RUN pip install --no-cache-dir \
    "duckdb>=1.2.0,<2.0.0" "duckdb-engine>=0.17.0" "starrocks>=1.0.6"
