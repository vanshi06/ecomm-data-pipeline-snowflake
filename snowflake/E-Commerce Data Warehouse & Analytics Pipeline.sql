--Database & Schema Setup
CREATE DATABASE IF NOT EXISTS ECOMM_DB;
USE DATABASE ECOMM_DB;

CREATE SCHEMA IF NOT EXISTS ECOMM_DB.RAW;
CREATE SCHEMA IF NOT EXISTS ECOMM_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS ECOMM_DB.CORE;
CREATE SCHEMA IF NOT EXISTS ECOMM_DB.REPORTING;
CREATE SCHEMA IF NOT EXISTS ECOMM_DB.AUDIT;

USE SCHEMA RAW;

--Storage Integration & Stage
CREATE OR REPLACE STORAGE INTEGRATION S3_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN = '<YOUR_AWS_IAM_ROLE_ARN>'
STORAGE_ALLOWED_LOCATIONS = ('s3://ecomm-project-1/');

DESC STORAGE INTEGRATION S3_INT;


-- File Format
CREATE OR REPLACE FILE FORMAT ECOMM_DB.RAW.CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('' , 'NULL')
EMPTY_FIELD_AS_NULL = TRUE;




--The Stage object
CREATE OR REPLACE STAGE S3_STAGE
URL = 's3://ecomm-project-1/'
STORAGE_INTEGRATION = S3_INT
FILE_FORMAT = CSV_FORMAT ;


-- Raw Layer Tables

CREATE OR REPLACE TABLE ECOMM_DB.RAW.CUSTOMERS_RAW(
customer_id    STRING,
customer_name  STRING,
region         STRING,
file_name      STRING,
load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ECOMM_DB.RAW.PRODUCTS_RAW(
product_id    STRING,
product_name  STRING,
category      STRING,
file_name     STRING,
load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ECOMM_DB.RAW.ORDERS_RAW(
order_ID    STRING,
customer_id    STRING,
product_id    STRING,
order_date   STRING,
quantity   STRING,
sales_amount   STRING,
file_name     STRING,
load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
-- Snowpipe (Auto-Ingest)

CREATE OR REPLACE PIPE ECOMM_DB.RAW.ORDERS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO ECOMM_DB.RAW.ORDERS_RAW (order_id, customer_id, product_id, order_date, quantity , sales_amount , file_name)
FROM @ECOMM_DB.RAW.S3_STAGE/orders/
FILE_FORMAT = (FORMAT_NAME = ECOMM_DB.RAW.CSV_FORMAT)
ON_ERROR = 'CONTINUE';


CREATE OR REPLACE PIPE ECOMM_DB.RAW.CUSTOMERS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO ECOMM_DB.RAW.CUSTOMERS_RAW (customer_id , customer_name , region , file_name)
from @ECOMM_DB.RAW.S3_STAGE/customers/
FILE_FORMAT = (FORMAT_NAME = ECOMM_DB.RAW.CSV_FORMAT)
ON_ERROR = 'CONTINUE' ;

CREATE OR REPLACE PIPE ECOMM_DB.RAW.PRODUCTS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO ECOMM_DB.RAW.PRODUCTS_RAW (product_id , product_name , category , file_name)
FROM @ECOMM_DB.RAW.S3_STAGE/products/
FILE_FORMAT = (FORMAT_NAME = ECOMM_DB.RAW.CSV_FORMAT)
ON_ERROR = 'CONTINUE';

SHOW PIPES IN SCHEMA ECOMM_DB.RAW;

DESC PIPE ECOMM_DB.RAW.ORDERS_PIPE;

 SELECT
SYSTEM$PIPE_STATUS('ECOMM_DB.RAW.ORDERS_PIPE');


SELECT COUNT(*)  FROM ECOMM_DB.RAW.CUSTOMERS_RAW;
SELECT COUNT(*) FROM ECOMM_DB.RAW.PRODUCTS_RAW;
SELECT  COUNT(*) FROM ECOMM_DB.RAW.ORDERS_RAW;



-- Staging Layer STAGING;
USE SCHEMA ECOMM_DB.STAGING;


CREATE OR REPLACE TABLE ECOMM_DB.STAGING.ORDERS_STG(
order_id       NUMBER,
customer_id    NUMBER,
product_id     NUMBER,
order_date     DATE,
quantity       NUMBER,
sales_amount   NUMBER(12,2)
);


CREATE OR REPLACE TABLE ECOMM_DB.STAGING.CUSTOMERS_STG(
customer_id    NUMBER,
customer_name  STRING, 
region         STRING
);

CREATE OR REPLACE TABLE ECOMM_DB.STAGING.PRODUCTS_STG(
product_id  NUMBER,
product_name STRING,
category     STRING
);

CREATE OR REPLACE TABLE ECOMM_DB.AUDIT.REJECTED_RECORDS(
reject_id     STRING  DEFAULT UUID_STRING(),
source_table  STRING,
raw_data      VARIANT,
reject_reason STRING,
rejected_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);



-- Dimension & Fact Tables (MERGE, SCD Type 1)

USE SCHEMA ECOMM_DB.CORE;

CREATE OR REPLACE TABLE ECOMM_DB.CORE.DIM_CUSTOMER(
customer_key    NUMBER AUTOINCREMENT,
customer_id     NUMBER,
customer_name   STRING,
region          STRING,
updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ECOMM_DB.CORE.DIM_PRODUCT(
product_key   NUMBER AUTOINCREMENT,
product_id    NUMBER,
product_name  STRING,
category      STRING,
updated_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ECOMM_DB.CORE.FACT_ORDER(
order_id      NUMBER,
customer_key  NUMBER,
product_key   NUMBER,
order_date    DATE,
quantity      NUMBER,
sales_amount  NUMBER(12,2),
load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);



-- Audit layer

CREATE OR REPLACE TABLE ECOMM_DB.AUDIT.PIPELINE_LOG  
(
    run_id          STRING,
    layer_name      STRING,
    status          STRING,
    rows_processed  NUMBER,
    rows_rejected   NUMBER,
    error_message   STRING,
    start_time      TIMESTAMP_NTZ,
    end_time        TIMESTAMP_NTZ
);


-- Stored Procedure


CREATE OR REPLACE PROCEDURE ECOMM_DB.RAW.SP_LOAD_CUSTOMERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id STRING DEFAULT UUID_STRING();
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_processed NUMBER DEFAULT 0;
    v_rows_rejected NUMBER DEFAULT 0;

BEGIN

    INSERT INTO ECOMM_DB.AUDIT.REJECTED_RECORDS
    (
        source_table,
        raw_data,
        reject_reason
    )
    SELECT
        'CUSTOMERS_RAW',
        OBJECT_CONSTRUCT(
            'customer_id', customer_id,
            'customer_name', customer_name,
            'region', region
        ),
        'INVALID CUSTOMER DATA'
    FROM ECOMM_DB.RAW.CUSTOMERS_RAW
    WHERE TRY_TO_NUMBER(customer_id) IS NULL
       OR NULLIF(TRIM(customer_name), '') IS NULL
       OR NULLIF(TRIM(region), '') IS NULL;

    v_rows_rejected := SQLROWCOUNT;


    INSERT INTO ECOMM_DB.STAGING.CUSTOMERS_STG
    (
        customer_id,
        customer_name,
        region
    )
    SELECT
        TRY_TO_NUMBER(customer_id),
        TRIM(customer_name),
        UPPER(TRIM(region))
    FROM ECOMM_DB.RAW.CUSTOMERS_RAW
    WHERE TRY_TO_NUMBER(customer_id) IS NOT NULL
      AND NULLIF(TRIM(customer_name), '') IS NOT NULL
      AND NULLIF(TRIM(region), '') IS NOT NULL

      AND NOT EXISTS
      (
          SELECT 1
          FROM ECOMM_DB.STAGING.CUSTOMERS_STG s
          WHERE s.customer_id = TRY_TO_NUMBER(customer_id)
            AND s.customer_name = TRIM(customer_name)
            AND s.region = UPPER(TRIM(region))
      )

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY customer_id
        ORDER BY load_timestamp DESC
    ) = 1;


    SELECT COUNT(*)
    INTO :v_rows_processed
    FROM ECOMM_DB.RAW.CUSTOMERS_RAW
    WHERE TRY_TO_NUMBER(customer_id) IS NOT NULL
      AND NULLIF(TRIM(customer_name), '') IS NOT NULL
      AND NULLIF(TRIM(region), '') IS NOT NULL;
 


    INSERT INTO ECOMM_DB.AUDIT.PIPELINE_LOG
    (
        run_id,
        layer_name,
        status,
        rows_processed,
        rows_rejected,
        error_message,
        start_time,
        end_time
    )
    VALUES
    (
        :v_run_id,
        'CUSTOMERS_STAGING',
        'SUCCESS',
        :v_rows_processed,
        :v_rows_rejected,
        NULL,
        :v_start_time,
        CURRENT_TIMESTAMP()
    );


    RETURN 'CUSTOMERS STAGING LOAD SUCCESS';

END;
$$;

CREATE OR REPLACE PROCEDURE ECOMM_DB.RAW.SP_LOAD_PRODUCTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id STRING DEFAULT UUID_STRING();
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_processed NUMBER DEFAULT 0;
    v_rows_rejected NUMBER DEFAULT 0;

BEGIN

    INSERT INTO ECOMM_DB.AUDIT.REJECTED_RECORDS
    (
        source_table,
        raw_data,
        reject_reason
    )
    SELECT
        'PRODUCTS_RAW',
        OBJECT_CONSTRUCT(
            'product_id', product_id,
            'product_name', product_name,
            'category', category
        ),
        'INVALID PRODUCT DATA'
    FROM ECOMM_DB.RAW.PRODUCTS_RAW
    WHERE TRY_TO_NUMBER(product_id) IS NULL
       OR NULLIF(TRIM(product_name), '') IS NULL
       OR NULLIF(TRIM(category), '') IS NULL;

    v_rows_rejected := SQLROWCOUNT;


    INSERT INTO ECOMM_DB.STAGING.PRODUCTS_STG
    (
        product_id,
        product_name,
        category
    )
    SELECT
        TRY_TO_NUMBER(product_id),
        TRIM(product_name),
        TRIM(category)
    FROM ECOMM_DB.RAW.PRODUCTS_RAW
    WHERE TRY_TO_NUMBER(product_id) IS NOT NULL
      AND NULLIF(TRIM(product_name), '') IS NOT NULL
      AND NULLIF(TRIM(category), '') IS NOT NULL

      AND NOT EXISTS
      (
          SELECT 1
          FROM ECOMM_DB.STAGING.PRODUCTS_STG s
          WHERE s.product_id = TRY_TO_NUMBER(product_id)
            AND s.product_name = TRIM(product_name)
            AND s.category = TRIM(category)
      )

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY product_id
        ORDER BY load_timestamp DESC
    ) = 1;

     SELECT COUNT(*)
    INTO :v_rows_processed
    FROM ECOMM_DB.RAW.PRODUCTS_RAW
    WHERE TRY_TO_NUMBER(product_id) IS NOT NULL
      AND NULLIF(TRIM(product_name), '') IS NOT NULL
      AND NULLIF(TRIM(category), '') IS NOT NULL;


    INSERT INTO ECOMM_DB.AUDIT.PIPELINE_LOG
    (
        run_id,
        layer_name,
        status,
        rows_processed,
        rows_rejected,
        error_message,
        start_time,
        end_time
    )
    VALUES
    (
        :v_run_id,
        'PRODUCTS_STAGING',
        'SUCCESS',
        :v_rows_processed,
        :v_rows_rejected,
        NULL,
        :v_start_time,
        CURRENT_TIMESTAMP()
    );


    RETURN 'PRODUCTS STAGING LOAD SUCCESS';

END;
$$;

CREATE OR REPLACE PROCEDURE ECOMM_DB.RAW.SP_LOAD_ORDERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id STRING DEFAULT UUID_STRING();
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_processed NUMBER DEFAULT 0;
    v_rows_rejected NUMBER DEFAULT 0;

BEGIN

    INSERT INTO ECOMM_DB.AUDIT.REJECTED_RECORDS
    (
        source_table,
        raw_data,
        reject_reason
    )
    SELECT
        'ORDERS_RAW',
        OBJECT_CONSTRUCT(
            'order_id', order_id,
            'customer_id', customer_id,
            'product_id', product_id,
            'order_date', order_date,
            'quantity', quantity,
            'sales_amount', sales_amount
        ),
        'INVALID ORDER DATA'
    FROM ECOMM_DB.RAW.ORDERS_RAW
    WHERE TRY_TO_NUMBER(order_id) IS NULL
       OR TRY_TO_NUMBER(customer_id) IS NULL
       OR TRY_TO_NUMBER(product_id) IS NULL
       OR TRY_TO_DATE(order_date) IS NULL
       OR TRY_TO_NUMBER(quantity) <= 0
       OR TRY_TO_DECIMAL(sales_amount, 12, 2) IS NULL
       OR TRY_TO_DECIMAL(sales_amount, 12, 2) < 0;

    v_rows_rejected := SQLROWCOUNT;


    INSERT INTO ECOMM_DB.STAGING.ORDERS_STG
    (
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        sales_amount
    )
    SELECT
        TRY_TO_NUMBER(order_id),
        TRY_TO_NUMBER(customer_id),
        TRY_TO_NUMBER(product_id),
        TRY_TO_DATE(order_date),
        TRY_TO_NUMBER(quantity),
        TRY_TO_DECIMAL(sales_amount, 12, 2)
    FROM ECOMM_DB.RAW.ORDERS_RAW
    WHERE TRY_TO_NUMBER(order_id) IS NOT NULL
      AND TRY_TO_NUMBER(customer_id) IS NOT NULL
      AND TRY_TO_NUMBER(product_id) IS NOT NULL
      AND TRY_TO_DATE(order_date) IS NOT NULL
      AND TRY_TO_NUMBER(quantity) > 0
      AND TRY_TO_DECIMAL(sales_amount, 12, 2) IS NOT NULL
      AND TRY_TO_DECIMAL(sales_amount, 12, 2) >= 0

      AND NOT EXISTS
      (
          SELECT 1
          FROM ECOMM_DB.STAGING.ORDERS_STG s
          WHERE s.order_id = TRY_TO_NUMBER(order_id)
      )

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY order_id
        ORDER BY load_timestamp DESC
    ) = 1;


   SELECT COUNT(*)
    INTO :v_rows_processed
    FROM ECOMM_DB.RAW.ORDERS_RAW
    WHERE TRY_TO_NUMBER(order_id) IS NOT NULL
      AND TRY_TO_NUMBER(customer_id) IS NOT NULL
      AND TRY_TO_NUMBER(product_id) IS NOT NULL
      AND TRY_TO_DATE(order_date) IS NOT NULL
      AND TRY_TO_NUMBER(quantity) > 0
      AND TRY_TO_DECIMAL(sales_amount, 12, 2) IS NOT NULL
      AND TRY_TO_DECIMAL(sales_amount, 12, 2) >= 0;


    INSERT INTO ECOMM_DB.AUDIT.PIPELINE_LOG
    (
        run_id,
        layer_name,
        status,
        rows_processed,
        rows_rejected,
        error_message,
        start_time,
        end_time
    )
    VALUES
    (
        :v_run_id,
        'ORDERS_STAGING',
        'SUCCESS',
        :v_rows_processed,
        :v_rows_rejected,
        NULL,
        :v_start_time,
        CURRENT_TIMESTAMP()
    );


    RETURN 'ORDERS STAGING LOAD SUCCESS';

END;
$$;





CREATE OR REPLACE PROCEDURE ECOMM_DB.CORE.SP_LOAD_DIM_FACT()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id STRING DEFAULT UUID_STRING();
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_processed NUMBER DEFAULT 0;

BEGIN

    -- Customer MERGE
    MERGE INTO ECOMM_DB.CORE.DIM_CUSTOMER AS tgt

USING
(
    SELECT
        customer_id,
        customer_name,
        region
    FROM ECOMM_DB.STAGING.CUSTOMERS_STG
    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY customer_id
        ORDER BY customer_id
    ) = 1
) AS src

ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN
    UPDATE SET
        customer_name = src.customer_name,
        region = src.region,
        updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT
    (
        customer_id,
        customer_name,
        region
    )
    VALUES
    (
        src.customer_id,
        src.customer_name,
        src.region
    );
   

    -- Product MERGE

    MERGE INTO ECOMM_DB.CORE.DIM_PRODUCT AS tgt

USING
(
    SELECT
        product_id,
        product_name,
        category
    FROM ECOMM_DB.STAGING.PRODUCTS_STG
    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY product_id
        ORDER BY product_id
    ) = 1
) AS src

ON tgt.product_id = src.product_id

WHEN MATCHED THEN
    UPDATE SET
        product_name = src.product_name,
        category = src.category,
        updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT
    (
        product_id,
        product_name,
        category
    )
    VALUES
    (
        src.product_id,
        src.product_name,
        src.category
    );
   

    -- Load Fact Table
    INSERT INTO ECOMM_DB.CORE.FACT_ORDER
(
    order_id,
    customer_key,
    product_key,
    order_date,
    quantity,
    sales_amount
)
SELECT
    o.order_id,
    c.customer_key,
    p.product_key,
    o.order_date,
    o.quantity,
    o.sales_amount
FROM ECOMM_DB.STAGING.ORDERS_STG o

JOIN ECOMM_DB.CORE.DIM_CUSTOMER c
    ON o.customer_id = c.customer_id

JOIN ECOMM_DB.CORE.DIM_PRODUCT p
    ON o.product_id = p.product_id

WHERE NOT EXISTS
(
    SELECT 1
    FROM ECOMM_DB.CORE.FACT_ORDER f
    WHERE f.order_id = o.order_id
);

    -- Count rows processed in this run
    v_rows_processed := SQLROWCOUNT;


    -- Pipeline Log
    INSERT INTO ECOMM_DB.AUDIT.PIPELINE_LOG
    (
        run_id,
        layer_name,
        status,
        rows_processed,
        rows_rejected,
        error_message,
        start_time,
        end_time
    )
    VALUES
    (
        :v_run_id,
        'CORE',
        'SUCCESS',
        :v_rows_processed,
        0,
        NULL,
        :v_start_time,
        CURRENT_TIMESTAMP()
    );


    RETURN 'CORE LOAD SUCCESS';

END;
$$;


-- BI-Ready Reporting Layer
-- View for Power BI

USE SCHEMA ECOMM_DB.REPORTING;

CREATE OR REPLACE VIEW ECOMM_DB.REPORTING.SALES_REPORT AS
SELECT
    f.order_id,
    f.order_date,
    c.customer_id,
    c.customer_name,
    c.region,
    p.product_id,
    p.product_name,
    p.category,
    f.quantity,
    f.sales_amount
FROM ECOMM_DB.CORE.FACT_ORDER f
JOIN ECOMM_DB.CORE.DIM_CUSTOMER c
    ON f.customer_key = c.customer_key
JOIN ECOMM_DB.CORE.DIM_PRODUCT p
    ON f.product_key = p.product_key;



--E-MAIL PROCEDURE

CREATE OR REPLACE PROCEDURE ECOMM_DB.AUDIT.SP_SEND_PIPELINE_EMAIL()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_customers NUMBER DEFAULT 0;
    v_products NUMBER DEFAULT 0;
    v_orders NUMBER DEFAULT 0;
    v_rejected NUMBER DEFAULT 0;
    v_email_body STRING;

BEGIN

    -- Latest Customers execution
    SELECT rows_processed
    INTO :v_customers
    FROM ECOMM_DB.AUDIT.PIPELINE_LOG
    WHERE layer_name = 'CUSTOMERS_STAGING'
    ORDER BY end_time DESC
    LIMIT 1;


    -- Latest Products execution
    SELECT rows_processed
    INTO :v_products
    FROM ECOMM_DB.AUDIT.PIPELINE_LOG
    WHERE layer_name = 'PRODUCTS_STAGING'
    ORDER BY end_time DESC
    LIMIT 1;


    -- Latest Orders execution
    SELECT rows_processed
    INTO :v_orders
    FROM ECOMM_DB.AUDIT.PIPELINE_LOG
    WHERE layer_name = 'ORDERS_STAGING'
    ORDER BY end_time DESC
    LIMIT 1;


    -- Latest rejected-record count
    SELECT rows_rejected
    INTO :v_rejected
    FROM ECOMM_DB.AUDIT.PIPELINE_LOG
    WHERE layer_name = 'ORDERS_STAGING'
    ORDER BY end_time DESC
    LIMIT 1;


    v_email_body :=
        'ECOMM PIPELINE EXECUTION SUMMARY' || CHR(10) || CHR(10) ||
        'Customers Processed: ' || v_customers || CHR(10) ||
        'Products Processed: ' || v_products || CHR(10) ||
        'Orders Processed: ' || v_orders || CHR(10) ||
        'Rejected Records: ' || v_rejected || CHR(10) ||
        'Pipeline Status: SUCCESS';


    CALL SYSTEM$SEND_EMAIL(
        'email_int',
        'vanshitaneja06@gmail.com',
        'ECOMM Pipeline Execution Summary',
        :v_email_body
    );


    RETURN 'PIPELINE EMAIL SENT SUCCESSFULLY';

END;
$$;


--CALL ECOMM_DB.AUDIT.SP_SEND_PIPELINE_EMAIL();

DESC TABLE ECOMM_DB.AUDIT.PIPELINE_LOG;

--TRUNCATE TABLE ECOMM_DB.AUDIT.PIPELINE_LOG;


 --Tasks 

CREATE OR REPLACE TASK ECOMM_DB.STAGING.TASK_LOAD_STAGING
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 6 * * * UTC'
AS
BEGIN
    CALL ECOMM_DB.RAW.SP_LOAD_CUSTOMERS();
    CALL ECOMM_DB.RAW.SP_LOAD_PRODUCTS();
    CALL ECOMM_DB.RAW.SP_LOAD_ORDERS();
END;

CREATE OR REPLACE TASK ECOMM_DB.STAGING.TASK_LOAD_DIM_FACT
WAREHOUSE = COMPUTE_WH
AFTER ECOMM_DB.STAGING.TASK_LOAD_STAGING
AS
CALL ECOMM_DB.CORE.SP_LOAD_DIM_FACT();

CREATE OR REPLACE TASK ECOMM_DB.STAGING.TASK_REFRESH_REPORTING
WAREHOUSE = COMPUTE_WH
AFTER ECOMM_DB.STAGING.TASK_LOAD_DIM_FACT
AS
SELECT 1;

CREATE OR REPLACE TASK ECOMM_DB.STAGING.TASK_SEND_PIPELINE_EMAIL
WAREHOUSE = COMPUTE_WH
AFTER ECOMM_DB.STAGING.TASK_REFRESH_REPORTING
AS
CALL ECOMM_DB.AUDIT.SP_SEND_PIPELINE_EMAIL();



ALTER TASK ECOMM_DB.STAGING.TASK_SEND_PIPELINE_EMAIL RESUME;

ALTER TASK ECOMM_DB.STAGING.TASK_REFRESH_REPORTING RESUME;

ALTER TASK ECOMM_DB.STAGING.TASK_LOAD_DIM_FACT RESUME;

ALTER TASK ECOMM_DB.STAGING.TASK_LOAD_STAGING RESUME;


SHOW TASKS IN SCHEMA ECOMM_DB.STAGING;

EXECUTE TASK ECOMM_DB.STAGING.TASK_LOAD_STAGING;


--Read-only role for the BI tool

CREATE ROLE IF NOT EXISTS BI_READONLY_ROLE;

GRANT USAGE ON DATABASE ECOMM_DB
TO ROLE BI_READONLY_ROLE;

GRANT USAGE ON SCHEMA ECOMM_DB.REPORTING
TO ROLE BI_READONLY_ROLE;

GRANT SELECT ON VIEW ECOMM_DB.REPORTING.SALES_REPORT
TO ROLE BI_READONLY_ROLE;

GRANT USAGE ON WAREHOUSE COMPUTE_WH
TO ROLE BI_READONLY_ROLE;

SHOW GRANTS TO ROLE BI_READONLY_ROLE;


