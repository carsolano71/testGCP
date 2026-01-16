from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import datetime

# =========================
# CONFIGURACIÓN
# =========================
PROJECT_ID = "my-project-cash-new"
BQ_LOCATION = "US"

# Bucket de Composer (defínelo como Variable en Airflow)
# Ejemplo de valor:
# us-central1-composer-poc-ca-0090c6dc-bucket
COMPOSER_BUCKET = Variable.get("COMPOSER_BUCKET")

# Ruta del SQL en GCS
SQL_GCS_PATH = (
    f"gs://{COMPOSER_BUCKET}/dags/sql/silver/customers.sql"
)

# =========================
# DAG
# =========================
with DAG(
    dag_id="example_customers_silver",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual (ideal para pruebas)
    catchup=False,
    tags=["silver", "customers", "bigquery"],
) as dag:

    load_customers_silver = BigQueryInsertJobOperator(
        task_id="load_customers_silver",
        configuration={
            "query": {
                # BigQuery lee el SQL directamente desde GCS
                "query": f"{{% include '{SQL_GCS_PATH}' %}}",
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
        project_id=PROJECT_ID,
    )

    load_customers_silver
