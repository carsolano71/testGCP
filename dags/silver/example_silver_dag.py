from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from pathlib import Path

# Ruta al SQL de Silver
SQL_PATH = Path(__file__).parents[2] / "sql" / "silver" / "customers_silver.sql"

with DAG(
    dag_id="customers_silver_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # ejecución manual (ideal para pruebas)
    catchup=False,
    tags=["silver", "customers"],
) as dag:

    run_customers_silver = BigQueryInsertJobOperator(
        task_id="run_customers_silver_sql",
        configuration={
            "query": {
                "query": SQL_PATH.read_text(),
                "useLegacySql": False,
            }
        },
        location="US",  # usa la región de tu BigQuery
    )

    run_customers_silver
