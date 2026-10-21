
MERGE `suresh-dataengineering.Golden_layer.Repurchased_curated_data` AS T

USING (
    SELECT *
    FROM `suresh-dataengineering.Silver_layer.Repurchase_stage_data`) AS S

ON  T.CUSTOMER_ID = S.CID
AND T.ORDER_ID = S.OID
AND T.WEB_PROD_ID = CAST(S.WEB_PROD_ID AS STRING)
AND T.IS_ACTIVE = TRUE

WHEN NOT MATCHED THEN
INSERT (
    CUSTOMER_ID,
    USER_PER_COUNT,
    ORDER_ID,
    PURCHASED_DATE,
    RECEIVED_DATE,
    ITEM_PRICE,
    QUANTITY,
    CUST_AMT,
    RECEIVED_QUANTITY,
    RECEIVED_AMOUNT,
    WEB_PROD_ID,
    GMM_DESC,
    PARENT_MDSE_DIVN_DESC,
    Loaded_timestamp,
    COUNTRY_CODE,
    ENTITY_ID,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    IS_ACTIVE
)
VALUES (
    S.CID,
    S.UPC,
    S.OID,
    S.DT,
    S.R_DT,
    S.PRC,
    S.QTY,
    S.AMT,
    S.R_QTY,
    S.R_AMT,
    CAST(S.WEB_PROD_ID AS STRING),
    S.GMM_DESC,
    S.PARENT_MDSE_DIVN_DESC,
    CURRENT_TIMESTAMP(),
    'IND', -- Confirm country mapping from DIS
    NULL,  -- Populate using the confirmed ENTITY_CODE rule
    CURRENT_TIMESTAMP(),
    TIMESTAMP '9999-12-31 23:59:59+00',
    TRUE
);
