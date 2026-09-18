-- =========================================================
-- ECOMM_DB PIPELINE VERIFICATION
-- (RAW load -> STAGING load -> CORE merge -> REPORTING)
-- =========================================================


-- ---------------------------------------------------------
-- 1. ROW COUNTS ACROSS ALL LAYERS
--    Single source of truth for RAW -> STAGING -> CORE counts.
--    Expect counts to shrink slightly at each layer as invalid
--    or duplicate rows are filtered out.
-- ---------------------------------------------------------
SELECT 'CUSTOMERS' AS entity, 'RAW' AS layer, COUNT(*) AS row_count FROM ECOMM_DB.RAW.CUSTOMERS_RAW
UNION ALL
SELECT 'CUSTOMERS', 'STAGING', COUNT(*) FROM ECOMM_DB.STAGING.CUSTOMERS_STG
UNION ALL
SELECT 'CUSTOMERS', 'CORE',    COUNT(*) FROM ECOMM_DB.CORE.DIM_CUSTOMER
UNION ALL
SELECT 'PRODUCTS',  'RAW',     COUNT(*) FROM ECOMM_DB.RAW.PRODUCTS_RAW
UNION ALL
SELECT 'PRODUCTS',  'STAGING', COUNT(*) FROM ECOMM_DB.STAGING.PRODUCTS_STG
UNION ALL
SELECT 'PRODUCTS',  'CORE',    COUNT(*) FROM ECOMM_DB.CORE.DIM_PRODUCT
UNION ALL
SELECT 'ORDERS',    'RAW',     COUNT(*) FROM ECOMM_DB.RAW.ORDERS_RAW
UNION ALL
SELECT 'ORDERS',    'STAGING', COUNT(*) FROM ECOMM_DB.STAGING.ORDERS_STG
UNION ALL
SELECT 'ORDERS',    'CORE',    COUNT(*) FROM ECOMM_DB.CORE.FACT_ORDER
ORDER BY entity, layer;


-- ---------------------------------------------------------
-- 2. DIMENSION & FACT SAMPLES
--    Confirms CORE layer holds real, well-formed data
--    (not just correct counts).
-- ---------------------------------------------------------
SELECT * FROM ECOMM_DB.CORE.DIM_CUSTOMER LIMIT 10;
SELECT * FROM ECOMM_DB.CORE.DIM_PRODUCT  LIMIT 10;
SELECT * FROM ECOMM_DB.CORE.FACT_ORDER LIMIT 10;

-- ---------------------------------------------------------
-- 3. DATA QUALITY: REJECTED RECORDS
--    Summary first, then a sample of actual bad rows caught.
-- ---------------------------------------------------------
SELECT
    source_table,
    reject_reason,
    COUNT(*) AS rejected_count
FROM ECOMM_DB.AUDIT.REJECTED_RECORDS
GROUP BY source_table, reject_reason
ORDER BY source_table;

SELECT * FROM ECOMM_DB.AUDIT.REJECTED_RECORDS LIMIT 15;


-- ---------------------------------------------------------
-- 4. REPORTING / BI LAYER
--    Final joined view that Power BI (or any BI tool) reads.
-- ---------------------------------------------------------
SELECT COUNT(*) FROM ECOMM_DB.REPORTING.SALES_REPORT;
SELECT * FROM ECOMM_DB.REPORTING.SALES_REPORT LIMIT 20;


-- ---------------------------------------------------------
-- 5. PIPELINE EXECUTION LOG
--    Proves the pipeline is re-runnable and self-logging.
-- ---------------------------------------------------------
SELECT
    layer_name,
    status,
    rows_processed,
    rows_rejected,
    start_time,
    end_time
FROM ECOMM_DB.AUDIT.PIPELINE_LOG
ORDER BY end_time DESC;


-- ---------------------------------------------------------
-- 6. TASK ORCHESTRATION
--    Filtered to this project's 4 tasks only (excludes
--    unrelated account-level Snowflake system tasks).
-- ---------------------------------------------------------
SELECT
    NAME,
    STATE,
    ERROR_MESSAGE
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
        RESULT_LIMIT => 20
    )
)
WHERE NAME IN (
    'TASK_LOAD_STAGING',
    'TASK_LOAD_DIM_FACT',
    'TASK_REFRESH_REPORTING',
    'TASK_SEND_PIPELINE_EMAIL'
)
ORDER BY QUERY_START_TIME DESC;


-- ---------------------------------------------------------
-- 7. SECURITY: BI READ-ONLY ROLE
--    Confirms least-privilege access for the BI/reporting role.
-- ---------------------------------------------------------
SHOW GRANTS TO ROLE BI_READONLY_ROLE;


-- ---------------------------------------------------------
-- 8. SNOWPIPE STATUS
--    Confirms auto-ingest pipes exist and are attached
--    to the current stage.
-- ---------------------------------------------------------
SHOW PIPES IN SCHEMA ECOMM_DB.RAW;


-- ---------------------------------------------------------
-- 9. EMAIL ALERTING
--    Task history specifically for the notification task.
--    Cross-check the timestamp here against the actual
--    email received in the configured inbox.
-- ---------------------------------------------------------
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'TASK_SEND_PIPELINE_EMAIL',
        RESULT_LIMIT => 10
    )
)
ORDER BY SCHEDULED_TIME DESC;
