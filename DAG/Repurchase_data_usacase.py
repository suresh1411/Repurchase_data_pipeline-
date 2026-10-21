
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# ==================================================
# CONFIGURATION
# ==================================================

PROJECT_ID = "suresh-dataengineering"
BUCKET_NAME = "source_landing_zone"
SOURCE_FILE = "Repurchase_Product.csv"
LOCATION = "US"

BRONZE_TABLE = (
    "suresh-dataengineering.Bronze_layer.Repurchase_raw_data"
)

SILVER_TABLE = (
    "suresh-dataengineering.Silver_layer.Repurchase_stage_data"
)

GOLDEN_TABLE = (
    "suresh-dataengineering.Golden_layer.Repurchased_curated_data"
)

# ==================================================
# SQL FILE PATHS
# Expected project structure:
# Repurchase_data_usecase/
#   DAG/Repurchase_data_usacase.py
#   SQL/Bronze.sql
#   SQL/Silver.sql
#   SQL/Golden_Create.sql
#   SQL/Golden_Expire.sql
#   SQL/Golden_Insert.sql
# ==================================================

DAG_DIR = Path(__file__).resolve().parent
SQL_DIR = DAG_DIR / "SQL"

def read_sql(filename):
    sql_file = SQL_DIR / filename

    if not sql_file.exists():
        raise FileNotFoundError(
            f"SQL file not found: {sql_file}"
        )

    return sql_file.read_text(encoding="utf-8")

# ==================================================
# DAG
# ==================================================

with DAG(
    dag_id="repurchase_data_pipeline",
    description="Repurchase CSV to BigQuery Bronze, Silver and Golden",
    start_date=datetime(2026, 10, 20),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["repurchase", "gcp", "bigquery"],
) as dag:

    # 1. Create Bronze table
    create_bronze = BigQueryInsertJobOperator(
        task_id="create_bronze_table",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": read_sql("Bronze.sql"),
                "useLegacySql": False,
            }
        },
    )

    # 2. Load CSV directly from GCS into Bronze
    load_bronze = GCSToBigQueryOperator(
        task_id="load_csv_directly_to_bronze",
        project_id=PROJECT_ID,
        bucket=BUCKET_NAME,
        source_objects=[SOURCE_FILE],
        destination_project_dataset_table=BRONZE_TABLE,
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=False,
        schema_fields=[
            {"name": "CID", "type": "STRING"},
            {"name": "UPC", "type": "STRING"},
            {"name": "OID", "type": "STRING"},
            {"name": "DT", "type": "STRING"},
            {"name": "R_DT", "type": "STRING"},
            {"name": "PRC", "type": "STRING"},
            {"name": "QTY", "type": "STRING"},
            {"name": "AMT", "type": "STRING"},
            {"name": "R_QTY", "type": "STRING"},
            {"name": "R_AMT", "type": "STRING"},
            {"name": "WEB_PROD_ID", "type": "STRING"},
            {"name": "GMM_DESC", "type": "STRING"},
            {"name": "PARENT_MDSE_DIVN_DESC", "type": "STRING"},
        ],
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_TRUNCATE",
        location=LOCATION,
    )

    # 3. Transform Bronze into Silver
    load_silver = BigQueryInsertJobOperator(
        task_id="load_silver",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": read_sql("Silver.sql"),
                "useLegacySql": False,
            }
        },
    )

    # 4. Create Golden table
    create_golden = BigQueryInsertJobOperator(
        task_id="create_golden_table",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": read_sql("Golden_Create.sql"),
                "useLegacySql": False,
            }
        },
    )

    # 5. Expire changed active Golden records
    expire_golden = BigQueryInsertJobOperator(
        task_id="expire_golden_records",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": read_sql("Golden_Expire.sql"),
                "useLegacySql": False,
            }
        },
    )

    # 6. Insert new active Golden records
    insert_golden = BigQueryInsertJobOperator(
        task_id="insert_golden_records",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": read_sql("Golden_Insert.sql"),
                "useLegacySql": False,
            }
        },
    )

    # ==================================================
    # TASK DEPENDENCIES
    # ==================================================

    (
        create_bronze
        >> load_bronze
        >> load_silver
        >> create_golden
        >> expire_golden
        >> insert_golden
    )
