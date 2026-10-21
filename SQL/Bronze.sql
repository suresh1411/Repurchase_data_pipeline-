CREATE TABLE IF NOT EXISTS
`suresh-dataengineering.Bronze_layer.Repurchase_raw_data`
(
    CID STRING,
    uPC STRING,
    OID STRING,
    DT STRING,
    R_DT STRING,
    PRC STRING,
    QTY STRING,
    AMT STRING,
    R_QTY STRING,
    R_AMT STRING,
    web_prod_id STRING,
    GMM_DESC STRING,
    PARENT_MDSE_DIVN_DESC STRING,
    CURRENT_TIMESTAMP() AS Loaded_timestamp 
)
