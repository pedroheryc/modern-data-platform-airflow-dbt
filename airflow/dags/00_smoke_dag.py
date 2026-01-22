from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _ensure_schemas() -> None:
    """
    Creates bronze/silver/gold schemas in the warehouse Postgres instance.
    Uses env vars injected via docker-compose.
    """
    host = os.getenv("WAREHOUSE_HOST", "postgres")
    port = os.getenv("WAREHOUSE_PORT", "5432")
    db = os.getenv("WAREHOUSE_DB", "platform")
    user = os.getenv("WAREHOUSE_USER", "postgres")
    password = os.getenv("WAREHOUSE_PASSWORD", "postgres")

    bronze = os.getenv("WAREHOUSE_SCHEMA_BRONZE", "bronze")
    silver = os.getenv("WAREHOUSE_SCHEMA_SILVER", "silver")
    gold = os.getenv("WAREHOUSE_SCHEMA_GOLD", "gold")

    # Create a temporary Airflow connection URI for PostgresHook
    # (Hook reads conn from env var AIRFLOW_CONN_* if present)
    # We'll use PostgresHook with a direct URI by monkeypatching via env.
    os.environ["AIRFLOW_CONN_WAREHOUSE_PG"] = (
        f"postgresql://{user}:{password}@{host}:{port}/{db}"
    )

    hook = PostgresHook(postgres_conn_id="warehouse_pg")

    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {bronze};",
        f"CREATE SCHEMA IF NOT EXISTS {silver};",
        f"CREATE SCHEMA IF NOT EXISTS {gold};",
        "CREATE TABLE IF NOT EXISTS bronze.healthcheck (id int primary key, created_at timestamptz default now());",
        "INSERT INTO bronze.healthcheck (id) VALUES (1) ON CONFLICT (id) DO NOTHING;",
    ]
    for sql in statements:
        hook.run(sql)


with DAG(
    dag_id="00_smoke_platform",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["platform", "smoke"],
) as dag:
    ensure_schemas = PythonOperator(
        task_id="ensure_schemas_and_healthcheck",
        python_callable=_ensure_schemas,
    )

    ensure_schemas
