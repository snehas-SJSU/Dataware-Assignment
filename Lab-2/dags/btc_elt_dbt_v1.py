"""
DAG: btc_elt_dbt_v1 (ELT only)
Purpose:
  - Run dbt (run → test → snapshot) after ETL finishes.
  - Uses Airflow Connection 'snowflake_conn' to set DBT_* env vars.
"""

from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

# Paths inside the Airflow container
# - dbt_project.yml lives under the project folder btc_elt
# - profiles.yml lives one level up in /opt/airflow/dbt
DBT_PROJECT_DIR = "/opt/airflow/dbt/btc_elt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

# Build env for dbt from Airflow connection (snowflake_conn)
conn = BaseHook.get_connection("snowflake_conn")
DBT_ENV = {
    "DBT_USER": conn.login,
    "DBT_PASSWORD": conn.password,
    "DBT_ACCOUNT": (conn.extra_dejson or {}).get("account"),
    "DBT_SCHEMA": conn.schema,         # build schema (e.g., ANALYTICS)
    "DBT_DATABASE": (conn.extra_dejson or {}).get("database"),
    "DBT_ROLE": (conn.extra_dejson or {}).get("role"),
    "DBT_WAREHOUSE": (conn.extra_dejson or {}).get("warehouse"),
    "DBT_TYPE": "snowflake",
}

with DAG(
    dag_id="btc_elt_dbt_v1",
    start_date=datetime(2025, 10, 1),
    schedule=None,                     # triggered by ETL DAG
    catchup=False,
    tags=["ELT", "dbt", "analytics"],
    description="ELT: dbt run/test/snapshot to build analytics indicators from RAW",
) as dag:

    # dbt run — builds models (e.g., ANALYTICS.FCT_BTC_INDICATORS)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"/home/airflow/.local/bin/dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=DBT_ENV,
    )

    # dbt test — executes schema/data tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"/home/airflow/.local/bin/dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=DBT_ENV,
    )

    # dbt snapshot — versions raw tables (dbt_valid_from/dbt_valid_to)
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"/home/airflow/.local/bin/dbt snapshot "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=DBT_ENV,
    )

    dbt_run >> dbt_test >> dbt_snapshot
