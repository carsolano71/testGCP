from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from pathlib import Path

SQL_PATH = Path(__file__).parents[2] / "sql" / "silver" / "example.sql"

with DAG(
    dag_id="example_silver_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "silver"],
) as dag:

    run_sql = BigQueryInsertJobOperator(
        task_id="run_example_sql",
        configuration={
            "query": {
                "query": SQL_PATH.read_text(),
                "useLegacySql": False,
            }
        },
        location="US",  # ajusta a tu región
    )

    run_sql
