Repurchase Product Data Pipeline

1. Project Overview

This project implements an end-to-end data engineering pipeline using Google Cloud Platform (GCP). The pipeline ingests repurchase product data from a CSV file stored in Google Cloud Storage (GCS), processes the data in BigQuery using the Bronze, Silver, and Golden architecture, and orchestrates the workflow using Cloud Composer (Apache Airflow).

The project is developed using Visual Studio Code, with separate SQL files for data processing and a Python DAG for orchestration.

2. Business Objective

Ingest repurchase product data from GCS into BigQuery.

Preserve the original source data in the Bronze layer.

Remove exact duplicate records and perform data type conversions in the Silver layer.

Load cleaned and curated data into the Golden layer.

Support historical tracking through effective dates and active-record flags, according to confirmed business rules.

Automate the pipeline using Cloud Composer and Apache Airflow.

Validate data quality and maintain consistency across processing layers.

3. Technology Stack

Google Cloud Platform (GCP)

Google Cloud Storage (GCS)

BigQuery

Cloud Composer

Apache Airflow

Python

SQL

Visual Studio Code

Git and GitHub

4. Source Data

Source file: Repurchase_Product.csv

GCS bucket: source_landing_zone

GCS file path: gs://source_landing_zone/Repurchase_Product.csv

The source dataset contains customer, order, purchase date, received date, price, quantity, amount, and product classification information.

Source Columns

CID — Customer ID

UPC — Source product/user-related identifier; confirm the precise business definition from the DIS

OID — Order ID

DT — Purchase date

R_DT — Received date

PRC — Item price

QTY — Purchased quantity

AMT — Customer amount

R_QTY — Received quantity

R_AMT — Received amount

WEB_PROD_ID — Web product ID

GMM_DESC — Merchandise description

PARENT_MDSE_DIVN_DESC — Parent merchandise division description

5. GCP Resources

Resource

Name

GCP Project

suresh-dataengineering

GCS Bucket

source_landing_zone

Bronze Dataset

Bronze_layer

Bronze Table

Repurchase_raw_data

Silver Dataset

Silver_layer

Silver Table

Repurchase_stage_data

Golden Dataset

Golden_layer

Golden Table

Repurchased_curated_data

Airflow DAG

repurchase_data_pipeline

The BigQuery dataset locations must be compatible with the configured query location in the DAG.

6. Project Architecture

Source CSV in GCS
        |
        v
Google Cloud Storage
Repurchase_Product.csv
        |
        v
BigQuery Bronze Layer
Repurchase_raw_data
        |
        v
BigQuery Silver Layer
Repurchase_stage_data
        |
        v
BigQuery Golden Layer
Repurchased_curated_data

Cloud Composer / Apache Airflow
orchestrates the pipeline

7. Repository Structure

Repurchase_data_usecase/
│
├── DAG/
│   └── Repurchase_data_usacase.py
│
├── DOCS/
│
├── SQL/
│   ├── DAG/
│   ├── Bronze.sql
│   ├── Silver.sql
│   ├── Golden_Create.sql
│   ├── Golden_Insert.sql
│   └── Golden_Expire.sql
│
├── Cloud_build.yaml
└── README.md

The DAG reads SQL files from the SQL directory. The directory structure must be preserved when deploying the project to Cloud Composer.

8. Data Processing Layers

8.1 Bronze Layer

Table: suresh-dataengineering.Bronze_layer.Repurchase_raw_data

The Bronze layer stores the raw source data from the CSV file.

Responsibilities:

Load the CSV directly from GCS into BigQuery.

Store the 13 source columns as STRING values.

Preserve source values without applying business transformations.

Keep the raw ingestion process separate from downstream data cleansing.

The current design does not include a Loaded_timestamp column in Bronze.

8.2 Silver Layer

Table: suresh-dataengineering.Silver_layer.Repurchase_stage_data

The Silver layer cleans and standardizes the raw data.

Responsibilities:

Convert numeric values using SAFE_CAST.

Parse purchase and received dates into DATE values.

Remove exact duplicate source records using ROW_NUMBER().

Apply the agreed data-quality filters.

Preserve legitimate customer orders and product lines.

Retain descriptive product fields as STRING values.

Deduplication uses the complete set of source attributes rather than partitioning by customer ID or order ID alone.

This prevents legitimate records from being removed simply because they share the same customer or order identifier.

Since Loaded_timestamp is not included in Bronze, Silver deduplication must not depend on that column.

8.3 Golden Layer

Table: suresh-dataengineering.Golden_layer.Repurchased_curated_data

The Golden layer stores the curated business-ready records.

Responsibilities:

Map Silver columns to the Golden target column names.

Maintain the required target data types.

Insert new records.

Expire changed records when SCD Type 2 processing is required.

Track effective start dates, effective end dates, and active-record status.

The Golden processing uses separate SQL files:

Golden_Create.sql — Creates the Golden table.

Golden_Expire.sql — Expires changed active records.

Golden_Insert.sql — Inserts new active records.

Important: The DIS identifies OID as a primary key, but repeated order IDs occur in the source data. The correct transaction-level business key must be confirmed before finalizing the Golden MERGE logic. Country and entity code mappings must also follow the approved DIS rules.

9. Apache Airflow DAG Workflow

The DAG is named repurchase_data_pipeline.

The planned execution sequence is:

create_bronze_table — Create the Bronze table.

load_csv_directly_to_bronze — Load the source CSV directly from GCS into Bronze.

load_silver — Execute the Silver SQL transformation.

create_golden_table — Create the Golden target table if it does not exist.

expire_golden_records — Expire changed active records.

insert_golden_records — Insert new active records.

The DAG uses BigQueryInsertJobOperator for SQL execution and GCSToBigQueryOperator for direct CSV ingestion.

The initial DAG configuration uses manual execution with schedule=None and catchup=False.

Bronze Load Behavior

The current DAG uses WRITE_TRUNCATE, which replaces the contents of the Bronze table during each successful load.

This is suitable for testing a single-file full refresh. If the pipeline later processes daily files, the ingestion strategy must be reviewed to support incremental processing and prevent the same file from being loaded repeatedly.

10. Prerequisites

Before running the pipeline, ensure that:

The GCP project and required BigQuery datasets exist.

The source CSV is available at gs://source_landing_zone/Repurchase_Product.csv.

The Bronze table schema matches the 13 CSV source columns.

The Cloud Composer environment has been created.

The Composer service account has the required GCS and BigQuery permissions.

The configured BigQuery location matches the dataset locations.

The SQL files use the correct dataset and table names.

The DAG and SQL files are deployed with the expected directory structure.

11. Deployment and Execution

Open the project in Visual Studio Code.

Review the SQL files and the Python DAG.

Confirm the source CSV headers match the Bronze schema.

Confirm that Silver SQL does not reference the removed Loaded_timestamp column.

Validate the Golden business key and SCD Type 2 rules.

Open the Cloud Composer environment in Google Cloud Console.

Locate the environment's DAG bucket.

Upload Repurchase_data_usacase.py and the required SQL directory, preserving their relative paths.

Open the Airflow UI.

Wait for repurchase_data_pipeline to appear.

Trigger the DAG manually.

Monitor the task statuses and inspect logs if any task fails.

Validate the data in BigQuery after execution.

12. Data Validation Queries

Check Bronze Record Count

SELECT COUNT(*) AS bronze_count
FROM `suresh-dataengineering.Bronze_layer.Repurchase_raw_data`;

Check Silver Record Count

SELECT COUNT(*) AS silver_count
FROM `suresh-dataengineering.Silver_layer.Repurchase_stage_data`;

Check Active Golden Records

SELECT COUNT(*) AS active_golden_count
FROM `suresh-dataengineering.Golden_layer.Repurchased_curated_data`
WHERE IS_ACTIVE = TRUE;

Identify Repeated Order IDs

SELECT
    OID,
    COUNT(*) AS row_count
FROM `suresh-dataengineering.Bronze_layer.Repurchase_raw_data`
GROUP BY OID
HAVING COUNT(*) > 1
ORDER BY row_count DESC;

Repeated order IDs must be investigated rather than automatically deleted because an order may contain multiple legitimate product lines.

13. Current Limitations and Pending Validation

Confirm the business key for Golden-layer MERGE operations.

Ensure all Silver SQL is compatible with the removal of Loaded_timestamp from Bronze.

Validate all Silver-to-Golden column mappings and data types.

Confirm the business rules for COUNTRY_CODE and ENTITY_ID.

Verify that Golden expiration and insertion logic correctly maintains SCD Type 2 history.

Implement file-reprocessing controls before switching to recurring daily ingestion.

Test the complete pipeline in Cloud Composer and validate all task results.

14. Project Status

Status: Development and testing in progress.

The source file location, project structure, and initial Bronze–Silver–Golden architecture have been established. Cloud Composer execution and end-to-end pipeline validation are pending.