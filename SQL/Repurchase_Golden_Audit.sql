
-- =====================================================
-- USE CASE: REPURCHASE DATA PIPELINE
-- LAYER: GOLDEN AUDIT
-- PURPOSE: AUDIT SCD TYPE 2 PROCESSING
-- =====================================================

-- STEP 1: CREATE GOLDEN AUDIT TABLE

CREATE TABLE IF NOT EXISTS
  `suresh-dataengineering.Golden_layer.Repurchase_Golden_Audit`
(
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
);

-- STEP 2: INSERT GOLDEN AUDIT RECORD

INSERT INTO
  `suresh-dataengineering.Golden_layer.Repurchase_Golden_Audit`
(
  USECASE_NAME,
  RUN_ID,
  SOURCE_TABLE,
  TARGET_TABLE,
  SILVER_COUNT,
  GOLDEN_INSERT_COUNT,
  GOLDEN_EXPIRE_COUNT,
  GOLDEN_ACTIVE_COUNT,
  LOAD_STATUS,
  START_TIME,
  END_TIME,
  ERROR_MESSAGE
)
SELECT
  'Repurchase Data Pipeline',
  GENERATE_UUID(),
  'Silver_layer.Repurchase_stage_data',
  'Golden_layer.Repurchased_curated_data',

  -- Total records available in Silver
  (
    SELECT COUNT(*)
    FROM `suresh-dataengineering.Silver_layer.Repurchase_stage_data`
  ),

  -- Actual inserted count must be captured from Golden_Insert.sql
  NULL,

  -- Actual expired count must be captured from Golden_Expire.sql
  NULL,

  -- Current active records in Golden
  (
    SELECT COUNT(*)
    FROM `suresh-dataengineering.Golden_layer.Repurchased_curated_data`
    WHERE IS_ACTIVE = TRUE
  ),

  'SUCCESS',
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  NULL;
