from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from pathlib import Path

# Ruta al SQL
SQL_PATH = Path(__file__).parents[2] / "sql" / "silver" / "customers.sql"

with DAG(
    dag_id="example_silver_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze-to-silver", "customers", "silver"],
) as dag:

    run_customers_silver = BigQueryInsertJobOperator(
        task_id="run_customers_silver",
        project_id="my-project-cash-new",
        location="US",
        configuration={
            "query": {
                "query": SQL_PATH.read_text(),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )
