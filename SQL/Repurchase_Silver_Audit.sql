
-- =====================================================
-- USE CASE: REPURCHASE DATA PIPELINE
-- LAYER: SILVER AUDIT
-- =====================================================

-- STEP 1: CREATE SILVER AUDIT TABLE

CREATE TABLE IF NOT EXISTS
  `suresh-dataengineering.Silver_layer.Repurchase_Silver_Audit`
(
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
);

-- STEP 2: INSERT SILVER AUDIT RECORD

INSERT INTO
  `suresh-dataengineering.Silver_layer.Repurchase_Silver_Audit`
(
  USECASE_NAME,
  RUN_ID,
  SOURCE_TABLE,
  TARGET_TABLE,
  BRONZE_COUNT,
  DUPLICATE_COUNT,
  SILVER_COUNT,
  REJECTED_COUNT,
  LOAD_STATUS,
  START_TIME,
  END_TIME,
  ERROR_MESSAGE
)
WITH counts AS (
  SELECT
    (SELECT COUNT(*)
     FROM `suresh-dataengineering.Bronze_layer.Repurchase_raw_data`)
       AS bronze_count,

    (SELECT COUNT(*)
     FROM `suresh-dataengineering.Silver_layer.Repurchase_stage_data`)
       AS silver_count,

    (SELECT COUNT(*)
     FROM `suresh-dataengineering.Bronze_layer.Repurchase_raw_data`
     WHERE SAFE_CAST(CID AS INT64) IS NULL)
       AS rejected_count
)
SELECT
  'Repurchase Data Pipeline',
  GENERATE_UUID(),
  'Bronze_layer.Repurchase_raw_data',
  'Silver_layer.Repurchase_stage_data',
  bronze_count,
  bronze_count - silver_count,
  silver_count,
  rejected_count,
  'SUCCESS',
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  NULL
FROM counts;
