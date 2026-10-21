
CREATE OR REPLACE TABLE
  `suresh-dataengineering.Silver_layer.Repurchase_stage_data`
AS

WITH deduplicated AS (
    SELECT
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
        PARENT_MDSE_DIVN_DESC,
        Loaded_timestamp
    FROM
        `suresh-dataengineering.Bronze_layer.Repurchase_raw_data`

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
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
        ORDER BY Loaded_timestamp DESC
    ) = 1
)

SELECT
    SAFE_CAST(CID AS INT64) AS CID,
    SAFE_CAST(UPC AS INT64) AS UPC,
    SAFE_CAST(OID AS INT64) AS OID,

    COALESCE(
    SAFE.PARSE_DATE('%m/%d/%Y', DT),  -- 08/11/2016
    SAFE.PARSE_DATE('%m-%d-%Y', DT),  -- 08-11-2016
    SAFE.PARSE_DATE('%d/%m/%Y', DT),  -- 11/08/2016
    SAFE.PARSE_DATE('%d-%m-%Y', DT)   -- 11-08-2016
    ) AS DT,

    COALESCE(
    SAFE.PARSE_DATE('%m/%d/%Y', R_DT),
    SAFE.PARSE_DATE('%m-%d-%Y', R_DT),
    SAFE.PARSE_DATE('%d/%m/%Y', R_DT),
    SAFE.PARSE_DATE('%d-%m-%Y', R_DT)
    ) AS R_DT,

    SAFE_CAST(PRC AS FLOAT64) AS PRC,
    SAFE_CAST(QTY AS INT64) AS QTY,
    SAFE_CAST(AMT AS FLOAT64) AS AMT,
    SAFE_CAST(R_QTY AS INT64) AS R_QTY,
    SAFE_CAST(R_AMT AS FLOAT64) AS R_AMT,
    SAFE_CAST(WEB_PROD_ID AS INT64) AS WEB_PROD_ID,

    GMM_DESC,
    PARENT_MDSE_DIVN_DESC,
    Loaded_timestamp

FROM deduplicated

WHERE SAFE_CAST(CID AS INT64) IS NOT NULL;


