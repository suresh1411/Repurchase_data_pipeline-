
-- =====================================================
-- USE CASE: REPURCHASE DATA PIPELINE
-- LAYER: BRONZE AUDIT
-- =====================================================

-- STEP 1: CREATE AUDIT TABLE

CREATE TABLE IF NOT EXISTS
  `suresh-dataengineering.Bronze_layer.Repurchase_Bronze_Audit`
(
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
);

-- STEP 2: INSERT AUDIT RECORD

INSERT INTO
  `suresh-dataengineering.Bronze_layer.Repurchase_Bronze_Audit`
(
  USECASE_NAME,
  RUN_ID,
  SOURCE_FILE,
  RECORDS_LOADED_THIS_RUN,
  TOTAL_RECORDS_IN_BRONZE,
  LOAD_STATUS,
  START_TIME,
  END_TIME,
  ERROR_MESSAGE
)
SELECT
  'Repurchase Data Pipeline',
  GENERATE_UUID(),
  'gs://source_landing_zone/repurchase_data.csv',

  -- Records readable from the source CSV
  (
    SELECT COUNT(*)
    FROM `suresh-dataengineering.Bronze_layer.Repurchase_source_external`
  ),

  -- Total records currently in Bronze
  (
    SELECT COUNT(*)
    FROM `suresh-dataengineering.Bronze_layer.Repurchase_raw_data`
  ),

  'SUCCESS',
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  NULL;
