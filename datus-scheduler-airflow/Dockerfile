FROM apache/airflow:2.10.4
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir "pyspark>=3.4.0" "pymysql>=1.1.0" "psycopg2-binary>=2.9.0" "duckdb>=1.2.0,<2.0.0" "duckdb-engine>=0.17.0"
