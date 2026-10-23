
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
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

BRONZE_AUDIT = (
    "suresh-dataengineering.Bronze_layer.Repurchase_Bronze_Audit"
)
SILVER_AUDIT = (
    "suresh-dataengineering.Silver_layer.Repurchase_Silver_Audit"
)
GOLDEN_AUDIT = (
    "suresh-dataengineering.Golden_layer.Repurchase_Golden_Audit"
)

USECASE_NAME = "Repurchase Data Pipeline"

DAG_DIR = Path(__file__).resolve().parent
SQL_DIR = DAG_DIR / "SQL"


# ==================================================
# COMMON FUNCTIONS
# ==================================================

def read_sql(filename):
    sql_file = SQL_DIR / filename

    if not sql_file.exists():
        raise FileNotFoundError(
            f"SQL file not found: {sql_file}"
        )

    return sql_file.read_text(encoding="utf-8")


def get_client():
    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
        location=LOCATION,
    )
    return hook.get_client(project_id=PROJECT_ID)


def get_count(client, table, condition=None):
    query = f"SELECT COUNT(*) AS record_count FROM `{table}`"

    if condition:
        query += f" WHERE {condition}"

    row = next(iter(
        client.query(query, location=LOCATION).result()
    ))
    return int(row["record_count"])


def get_task_run_details(context, task_id):
    """Get a task instance from the current DAG run."""
    dag_run = context["dag_run"]

    task_instance = dag_run.get_task_instance(
        task_id=task_id,
        map_index=-1,
    )

    if task_instance is None:
        raise ValueError(
            f"Task instance not found: {task_id}"
        )

    return task_instance


def get_task_job_id(context, task_id):
    """Read and validate the BigQuery job ID returned through XCom."""
    ti = context["ti"]

    job_id = ti.xcom_pull(
        task_ids=task_id,
        key="return_value",
    )

    # Handle providers that return a dictionary.
    if isinstance(job_id, dict):
        job_id = job_id.get("job_id")

    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError(
            f"BigQuery job ID not found in XCom for {task_id}. "
            "Check the operator logs and provider version."
        )

    return job_id


def get_dml_affected_rows(client, context, task_id):
    job_id = get_task_job_id(context, task_id)

    job = client.get_job(
        job_id,
        project=PROJECT_ID,
        location=LOCATION,
    )

    job.result()

    affected_rows = job.num_dml_affected_rows

    if affected_rows is None:
        raise ValueError(
            f"BigQuery did not return DML affected-row statistics "
            f"for job {job_id}"
        )

    return int(affected_rows)


def insert_audit(client, table, columns, values):
    """Insert an audit row using BigQuery query parameters."""
    from google.cloud import bigquery

    placeholders = ", ".join(
        f"@p{i}" for i in range(len(values))
    )
    column_sql = ", ".join(columns)

    query = f"""
        INSERT INTO `{table}` ({column_sql})
        VALUES ({placeholders})
    """

    parameters = []

    for i, value in enumerate(values):
        if isinstance(value, datetime):
            parameters.append(
                bigquery.ScalarQueryParameter(
                    f"p{i}", "TIMESTAMP", value
                )
            )
        elif isinstance(value, bool):
            parameters.append(
                bigquery.ScalarQueryParameter(
                    f"p{i}", "BOOL", value
                )
            )
        elif isinstance(value, int):
            parameters.append(
                bigquery.ScalarQueryParameter(
                    f"p{i}", "INT64", value
                )
            )
        elif isinstance(value, float):
            parameters.append(
                bigquery.ScalarQueryParameter(
                    f"p{i}", "FLOAT64", value
                )
            )
        else:
            parameters.append(
                bigquery.ScalarQueryParameter(
                    f"p{i}", "STRING", value
                )
            )

    job_config = bigquery.QueryJobConfig(
        query_parameters=parameters
    )

    client.query(
        query,
        job_config=job_config,
        location=LOCATION,
    ).result()


def utc_now():
    return datetime.now(timezone.utc)


# ==================================================
# AUDIT TABLE CREATION
# ==================================================

def create_audit_tables():
    client = get_client()

    statements = [
        f"""
        CREATE TABLE IF NOT EXISTS `{BRONZE_AUDIT}` (
            USECASE_NAME STRING,
            RUN_ID STRING,
            SOURCE_FILE STRING,
            RECORDS_LOADED_THIS_RUN INT64,
            TOTAL_RECORDS_IN_BRONZE INT64,
            LOAD_STATUS STRING,
            START_TIME TIMESTAMP,
            END_TIME TIMESTAMP,
            ERROR_MESSAGE STRING,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS `{SILVER_AUDIT}` (
            USECASE_NAME STRING,
            RUN_ID STRING,
            SOURCE_TABLE STRING,
            TARGET_TABLE STRING,
            BRONZE_COUNT INT64,
            DUPLICATE_COUNT INT64,
            SILVER_COUNT INT64,
            REJECTED_COUNT INT64,
            LOAD_STATUS STRING,
            START_TIME TIMESTAMP,
            END_TIME TIMESTAMP,
            ERROR_MESSAGE STRING,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS `{GOLDEN_AUDIT}` (
            USECASE_NAME STRING,
            RUN_ID STRING,
            SOURCE_TABLE STRING,
            TARGET_TABLE STRING,
            SILVER_COUNT INT64,
            GOLDEN_INSERT_COUNT INT64,
            GOLDEN_EXPIRE_COUNT INT64,
            GOLDEN_ACTIVE_COUNT INT64,
            LOAD_STATUS STRING,
            START_TIME TIMESTAMP,
            END_TIME TIMESTAMP,
            ERROR_MESSAGE STRING,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """,
    ]

    for statement in statements:
        client.query(
            statement,
            location=LOCATION,
        ).result()


# ==================================================
# BRONZE AUDIT
# ==================================================

def record_bronze_audit(**context):
    client = get_client()

    load_task = get_task_run_details(
        context,
        "load_csv_directly_to_bronze",
    )

    if load_task.state != "success":
        raise ValueError(
            f"Bronze load did not succeed: {load_task.state}"
        )

    # Bronze is WRITE_TRUNCATE, so the table count after
    # a successful load represents the current file's rows.
    bronze_count = get_count(client, BRONZE_TABLE)

    start_time = load_task.start_date or utc_now()
    end_time = load_task.end_date or utc_now()

    insert_audit(
        client,
        BRONZE_AUDIT,
        [
            "USECASE_NAME",
            "RUN_ID",
            "SOURCE_FILE",
            "RECORDS_LOADED_THIS_RUN",
            "TOTAL_RECORDS_IN_BRONZE",
            "LOAD_STATUS",
            "START_TIME",
            "END_TIME",
            "ERROR_MESSAGE",
        ],
        [
            USECASE_NAME,
            context["run_id"],
            f"gs://{BUCKET_NAME}/{SOURCE_FILE}",
            bronze_count,
            bronze_count,
            "SUCCESS",
            start_time,
            end_time,
            None,
        ],
    )

    print(f"Bronze count: {bronze_count}")
    print("Bronze audit inserted successfully.")


# ==================================================
# SILVER AUDIT
# ==================================================

def record_silver_audit(**context):
    client = get_client()

    silver_task = get_task_run_details(
        context,
        "load_silver",
    )

    if silver_task.state != "success":
        raise ValueError(
            f"Silver load did not succeed: {silver_task.state}"
        )

    bronze_count = get_count(client, BRONZE_TABLE)
    silver_count = get_count(client, SILVER_TABLE)

    # Count exact duplicate rows using the source columns
    # used by the deduplication logic in Silver.sql.
    duplicate_query = f"""
        SELECT
            COUNT(*) -
            COUNT(DISTINCT TO_JSON_STRING(STRUCT(
                CID,
                UPC,
                OID,
                DT,
                R_DT,
                PRC,
                QTY,
                AMT,
                R_QTY,
                R_AMT,
                WEB_PROD_ID,
                GMM_DESC,
                PARENT_MDSE_DIVN_DESC
            ))) AS duplicate_count
        FROM `{BRONZE_TABLE}`
    """

    duplicate_row = next(iter(
        client.query(
            duplicate_query,
            location=LOCATION,
        ).result()
    ))
    duplicate_count = int(duplicate_row["duplicate_count"])

    # Match the invalid-CID filter in Silver.sql.
    rejected_count = get_count(
        client,
        BRONZE_TABLE,
        "SAFE_CAST(CID AS INT64) IS NULL",
    )

    start_time = silver_task.start_date or utc_now()
    end_time = silver_task.end_date or utc_now()

    insert_audit(
        client,
        SILVER_AUDIT,
        [
            "USECASE_NAME",
            "RUN_ID",
            "SOURCE_TABLE",
            "TARGET_TABLE",
            "BRONZE_COUNT",
            "DUPLICATE_COUNT",
            "SILVER_COUNT",
            "REJECTED_COUNT",
            "LOAD_STATUS",
            "START_TIME",
            "END_TIME",
            "ERROR_MESSAGE",
        ],
        [
            USECASE_NAME,
            context["run_id"],
            BRONZE_TABLE,
            SILVER_TABLE,
            bronze_count,
            duplicate_count,
            silver_count,
            rejected_count,
            "SUCCESS",
            start_time,
            end_time,
            None,
        ],
    )

    print(f"Bronze count: {bronze_count}")
    print(f"Exact duplicate count: {duplicate_count}")
    print(f"Rejected CID count: {rejected_count}")
    print(f"Silver count: {silver_count}")
    print("Silver audit inserted successfully.")


# ==================================================
# GOLDEN AUDIT
# ==================================================

def record_golden_audit(**context):
    client = get_client()

    expire_task = get_task_run_details(
        context,
        "expire_golden_records",
    )
    insert_task = get_task_run_details(
        context,
        "insert_golden_records",
    )

    if expire_task.state != "success":
        raise ValueError(
            f"Golden expire task failed: {expire_task.state}"
        )

    if insert_task.state != "success":
        raise ValueError(
            f"Golden insert task failed: {insert_task.state}"
        )

    # Read the actual BigQuery DML statistics for this run.
    expired_count = get_dml_affected_rows(
        client,
        context,
        "expire_golden_records",
    )
    inserted_count = get_dml_affected_rows(
        client,
        context,
        "insert_golden_records",
    )

    silver_count = get_count(client, SILVER_TABLE)
    active_count = get_count(
        client,
        GOLDEN_TABLE,
        "IS_ACTIVE = TRUE",
    )

    start_time = expire_task.start_date or utc_now()
    end_time = insert_task.end_date or utc_now()

    insert_audit(
        client,
        GOLDEN_AUDIT,
        [
            "USECASE_NAME",
            "RUN_ID",
            "SOURCE_TABLE",
            "TARGET_TABLE",
            "SILVER_COUNT",
            "GOLDEN_INSERT_COUNT",
            "GOLDEN_EXPIRE_COUNT",
            "GOLDEN_ACTIVE_COUNT",
            "LOAD_STATUS",
            "START_TIME",
            "END_TIME",
            "ERROR_MESSAGE",
        ],
        [
            USECASE_NAME,
            context["run_id"],
            SILVER_TABLE,
            GOLDEN_TABLE,
            silver_count,
            inserted_count,
            expired_count,
            active_count,
            "SUCCESS",
            start_time,
            end_time,
            None,
        ],
    )

    print(f"Silver count: {silver_count}")
    print(f"Golden inserted count: {inserted_count}")
    print(f"Golden expired count: {expired_count}")
    print(f"Golden active count: {active_count}")
    print("Golden audit inserted successfully.")


# ==================================================
# DAG DEFINITION
# ==================================================

with DAG(
    dag_id="repurchase_data_pipeline",
    description=(
        "Repurchase CSV to BigQuery Bronze, Silver and Golden "
        "with per-run audit records"
    ),
    start_date=datetime(2026, 10, 20),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["repurchase", "gcp", "bigquery", "audit"],
) as dag:

    # Create the audit tables before starting the pipeline.
    create_audit_tables_task = PythonOperator(
        task_id="create_repurchase_audit_tables",
        python_callable=create_audit_tables,
    )

    # ------------------------------
    # BRONZE
    # ------------------------------

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
            {
                "name": "PARENT_MDSE_DIVN_DESC",
                "type": "STRING",
            },
        ],
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_TRUNCATE",
        location=LOCATION,
    )

    bronze_audit = PythonOperator(
        task_id="record_repurchase_bronze_audit",
        python_callable=record_bronze_audit,
    )

    # ------------------------------
    # SILVER
    # ------------------------------

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

    silver_audit = PythonOperator(
        task_id="record_repurchase_silver_audit",
        python_callable=record_silver_audit,
    )

    # ------------------------------
    # GOLDEN / SCD TYPE 2
    # ------------------------------

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

    golden_audit = PythonOperator(
        task_id="record_repurchase_golden_audit",
        python_callable=record_golden_audit,
    )

    # ==================================================
    # TASK DEPENDENCIES
    # ==================================================

    create_audit_tables_task >> create_bronze

    create_bronze >> load_bronze >> bronze_audit

    bronze_audit >> load_silver >> silver_audit

    silver_audit >> create_golden

    
    create_golden >> expire_golden >> insert_golden >> golden_audit
