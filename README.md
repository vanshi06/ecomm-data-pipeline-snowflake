# E-Commerce Data Warehouse & Analytics Pipeline — Snowflake & AWS

## **1. Project Overview**

This project implements an end-to-end **data engineering pipeline** using **Amazon S3, Snowflake, and Snowpipe auto-ingestion**.

E-commerce data (customers, products, and orders) is ingested from Amazon S3 into Snowflake automatically via Snowpipe, validated and cleaned through stored procedures, merged into a dimensional model (SCD Type 1), and exposed through a BI-ready reporting view. The pipeline is fully orchestrated with Snowflake Tasks, self-logs every run for auditability, and sends automated email alerts on completion.

---

## **2. Project Objectives**

1. Auto-ingest e-commerce data from Amazon S3 into Snowflake using Snowpipe.
2. Build a structured Snowflake data warehouse across RAW, STAGING, CORE, REPORTING, and AUDIT schemas.
3. Validate and clean incoming data, rejecting and logging malformed records.
4. Build a dimensional model (customer and product dimensions, order fact table) with incremental MERGE logic.
5. Expose a BI-ready reporting view for tools like Power BI.
6. Orchestrate the full pipeline using chained Snowflake Tasks.
7. Log every pipeline run for auditability and re-runnability.
8. Send automated email alerts summarizing each run.
9. Implement least-privilege, read-only access for BI consumers.
10. Secure cross-account access between AWS and Snowflake via IAM role trust.

---

## **3. Architecture**

```
        Amazon S3
    (customers/, products/, orders/)
           │
           ▼
   Snowpipe (Auto-Ingest)
           │
           ▼
      RAW Layer
   (CUSTOMERS_RAW, PRODUCTS_RAW, ORDERS_RAW)
           │
           ▼
   Validation & Rejection
   ┌───────┴────────┐
   │                │
Rejected Records   Clean Rows
(AUDIT schema)         │
                       ▼
                STAGING Layer
                       │
                       ▼
                  CORE Layer
             ┌─────────┴─────────┐
             │                   │
       DIM_CUSTOMER          DIM_PRODUCT
             │                   │
             └─────────┬─────────┘
                        ▼
                   FACT_ORDER
                        │
                        ▼
              REPORTING Layer
              (SALES_REPORT view)
                        │
              ┌─────────┴─────────┐
              │                   │
         Power BI / BI Tools   Email Alert
```

---

## **4. Technology Stack**

| Technology | Purpose |
|---|---|
| **Amazon S3** | Source data storage for customers, products, and orders CSVs |
| **AWS IAM** | Cross-account role-based trust allowing Snowflake to access S3 |
| **Snowflake** | Cloud data warehouse, ingestion, transformation, and orchestration |
| **Snowpipe** | Automated, event-driven ingestion from S3 into Snowflake |
| **SQL / Snowflake Scripting** | Stored procedures, validation logic, and MERGE-based transformations |
| **Snowflake Tasks** | Pipeline orchestration and scheduling |
| **GitHub** | Version control and project documentation |

---

## **5. Source Data**

The project works with three e-commerce datasets, delivered as CSV files to Amazon S3:

1. **Customers** — customer ID, name, and region
2. **Products** — product ID, name, and category
3. **Orders** — order ID, customer ID, product ID, order date, quantity, and sales amount, delivered across **three incremental batch files** (`orders_batch1.csv`, `orders_batch2.csv`, `orders_batch3.csv`) simulating daily Snowpipe drops, including intentionally repeated `order_id`s across batches to validate incremental load correctness.

The datasets also include intentionally malformed rows (blank fields, invalid IDs, bad dates, negative quantities) to exercise the pipeline's data-quality validation.

---

## **6. Snowflake Implementation**

### **6.1 Database**
```
ECOMM_DB
```

### **6.2 Schemas**
```
RAW
STAGING
CORE
REPORTING
AUDIT
```

### **6.3 Snowflake Features Used**
1. Storage integrations and external stages
2. Snowpipe with `AUTO_INGEST = TRUE`
3. CSV file formats
4. Stored procedures (SQL scripting)
5. `MERGE` statements for SCD Type 1 dimension loads
6. `QUALIFY ROW_NUMBER()` for deduplication
7. Chained Tasks with `AFTER` dependencies
8. `SYSTEM$SEND_EMAIL` for automated alerting
9. Role-based access control (least-privilege BI role)
10. `INFORMATION_SCHEMA.TASK_HISTORY` and `COPY_HISTORY` for observability

### **6.4 S3 Integration**

Snowflake connects to Amazon S3 through a storage integration backed by IAM role trust:

```
Amazon S3 (ecomm-project-1)
      ↓
Storage Integration (S3_INT)
      ↓
External Stage (S3_STAGE)
      ↓
Snowpipe (auto-ingest)
      ↓
Snowflake RAW Tables
```

The IAM role `project_admin` trusts Snowflake's IAM user via a scoped `sts:AssumeRole` policy — Snowflake never uses static AWS credentials.

---

## **7. Raw Data Layer**

```
CUSTOMERS_RAW
PRODUCTS_RAW
ORDERS_RAW
```

All RAW tables store data as `STRING` type with `file_name` and `load_timestamp` metadata columns, so no data is lost or type-coerced before validation. Three Snowpipes (`CUSTOMERS_PIPE`, `PRODUCTS_PIPE`, `ORDERS_PIPE`) auto-ingest new files as they land in their respective S3 folders.

---

## **8. Data Validation & Rejected Records**

Each entity has a dedicated stored procedure (`SP_LOAD_CUSTOMERS`, `SP_LOAD_PRODUCTS`, `SP_LOAD_ORDERS`) that:

1. Identifies invalid rows (non-numeric IDs, blank required fields, invalid dates, non-positive quantities, negative amounts) and inserts them into `AUDIT.REJECTED_RECORDS` with the reason and original raw data (as JSON).
2. Loads only valid, deduplicated rows into the STAGING layer, using `QUALIFY ROW_NUMBER()` to keep the latest version of any duplicate.

This two-path design means the pipeline never silently drops or corrupts data — every rejected row is captured with a reason and timestamp for review.

---

## **9. Staging Layer**

```
CUSTOMERS_STG
PRODUCTS_STG
ORDERS_STG
```

Typed, validated, deduplicated data — customer/product/order IDs as `NUMBER`, order dates as `DATE`, sales amounts as `NUMBER(12,2)`.

---

## **10. Core Layer (Dimensional Model, SCD Type 1)**

```
DIM_CUSTOMER
DIM_PRODUCT
FACT_ORDER
```

`SP_LOAD_DIM_FACT` performs `MERGE` operations into both dimension tables (updating existing records, inserting new ones), then inserts new orders into `FACT_ORDER` — joining against both dimensions and using `NOT EXISTS` to guarantee no duplicate fact rows, even across repeated incremental loads.

---

## **11. Reporting Layer**

```
REPORTING.SALES_REPORT
```

A single denormalized view joining `FACT_ORDER`, `DIM_CUSTOMER`, and `DIM_PRODUCT` — ready for direct consumption by Power BI or any other BI tool.

---

## **12. Audit & Observability**

```
AUDIT.REJECTED_RECORDS
AUDIT.PIPELINE_LOG
```

Every procedure run logs its `run_id`, layer name, status, rows processed, rows rejected, and start/end time to `PIPELINE_LOG` — making every run traceable and re-runnable without ambiguity.

---

## **13. Orchestration (Tasks)**

Four chained Snowflake Tasks automate the full pipeline:

```
TASK_LOAD_STAGING
      ↓ (AFTER)
TASK_LOAD_DIM_FACT
      ↓ (AFTER)
TASK_REFRESH_REPORTING
      ↓ (AFTER)
TASK_SEND_PIPELINE_EMAIL
```

`TASK_LOAD_STAGING` runs on a daily CRON schedule and calls all three staging-load procedures; each subsequent task fires automatically once its predecessor succeeds.

---

## **14. Email Alerting**

`SP_SEND_PIPELINE_EMAIL` queries the latest `PIPELINE_LOG` entries and sends a summary email via `SYSTEM$SEND_EMAIL`, reporting customers/products/orders processed and rejected-record counts for the most recent run.

---

## **15. Security: Least-Privilege BI Access**

```
BI_READONLY_ROLE
```

A dedicated role granted only `USAGE` on the database/schema/warehouse and `SELECT` on the `SALES_REPORT` view — BI tools never get write access or visibility into RAW/STAGING/CORE internals.

---

## **16. AWS Infrastructure**

### **16.1 S3 Bucket**
```
ecomm-project-1 (ap-south-1)
├── customers/
├── products/
└── orders/
```

### **16.2 IAM Role Trust**

The `project_admin` IAM role's trust policy scopes `sts:AssumeRole` specifically to Snowflake's IAM user ARN (obtained via `DESC STORAGE INTEGRATION`), rather than allowing broad or unscoped access — a deliberate least-privilege design choice for the cross-account connection.

---

## **17. Screenshots**

The pipeline's data quality, orchestration, and security were validated end-to-end in Snowflake, with source data ingested from a version-controlled AWS S3 bucket. Highlights below; the full validation set is in the collapsible section.

### Data Quality & Pipeline Flow

**Row counts across all layers** — proves data flows correctly through RAW → STAGING → CORE, with counts shrinking as invalid or duplicate rows are filtered out.

![Row counts across all layers](screenshots/sf/01_row_counts_all_layers.jpeg)

**Rejected records summary** — shows the validation logic actually catching bad data.

![Rejected records summary](screenshots/sf/02_rejected_records_summary.jpeg)

**Final reporting view** — the BI-ready `SALES_REPORT` view joining customers, products, and orders.

![Sales report sample](screenshots/sf/07_sales_report_sample.jpeg)

### Orchestration & Monitoring

**Task chain execution** — all four tasks succeeding in sequence.

![Task history filtered](screenshots/sf/09_task_history_filtered.jpeg)

**Pipeline execution log** — every run is self-logging and auditable.

![Pipeline log](screenshots/sf/08_pipeline_log.jpeg)

**Automated email alert** — the pipeline notifies a human on completion.

![Pipeline email confirmation](screenshots/sf/15_pipeline_email_confirmation.jpeg)

### Infrastructure: S3 & IAM

**S3 bucket structure** — three folders, with three incremental order batch files.

![S3 bucket root](screenshots/aws/05_s3_bucket_root.jpeg)

**Cross-account IAM trust** — scoped `sts:AssumeRole` policy, not an open trust relationship.

![IAM trust relationship](screenshots/aws/02_iam_trust_relationship.jpeg)

<details>
<summary><strong>Click to see additional validation screenshots</strong></summary>

#### Snowflake — Data Layers
![Rejected records sample](screenshots/sf/03_rejected_records_sample.jpeg)
![Dim customer sample](screenshots/sf/04_dim_customer_sample.jpeg)
![Dim product sample](screenshots/sf/05_dim_product_sample.jpeg)
![Fact order sample](screenshots/sf/06_fact_order_sample.jpeg)

#### Snowflake — Security, Ingestion & Alerting
![BI role grants](screenshots/sf/10_bi_role_grants.jpeg)
![Snowpipe status](screenshots/sf/11_snowpipe_status.jpeg)
![Email task history](screenshots/sf/12_email_task_history.jpeg)
![Orders pipe live status](screenshots/sf/13_orders_pipe_live_status.jpeg)
![Storage integration details](screenshots/sf/14_storage_integration_details.jpeg)

#### AWS — IAM & S3
![IAM role summary](screenshots/aws/01_iam_role_summary.jpeg)
![IAM role policy detail](screenshots/aws/03_iam_role_policy_detail.jpeg)
![IAM user summary](screenshots/aws/04_iam_user_summary.jpeg)
![S3 customers folder](screenshots/aws/06_s3_customers_folder.jpeg)
![S3 products folder](screenshots/aws/07_s3_products_folder.jpeg)
![S3 orders folder](screenshots/aws/08_s3_orders_folder.jpeg)
![S3 bucket properties](screenshots/aws/09_s3_bucket_properties.jpeg)

</details>

---

## **18. Repository Structure**

```
├── Snowflake/
│   ├── ecomm_pipeline_setup.sql
│   └── verify_pipeline.sql
│
├── Screenshot/
│   ├── sf/
│   │   └── (15 Snowflake validation screenshots)
│   └── aws/
│       └── (9 AWS IAM & S3 screenshots)
│
├── LICENSE
└── README.md
```

---

## **19. Challenges & Debugging**

This project involved genuine troubleshooting beyond initial setup:

- **Pipe/stage disconnection:** Recreating the external stage (`CREATE OR REPLACE STAGE`) after pipes were already created caused all three Snowpipes to halt with `STOPPED_STAGE_DROPPED`. Diagnosed via `SYSTEM$PIPE_STATUS`, fixed by recreating the pipes to reattach them to the current stage.
- **Duplicate ingestion after pipe recreation:** Recreating a pipe resets its internal "already-loaded files" tracking, causing a previously-loaded file to be re-ingested and doubling row counts. Resolved by truncating the affected RAW table before a final, clean refresh.
- **Incremental load validation:** Verified that `FACT_ORDER` correctly avoided duplicate rows across three overlapping order batch files (`orders_batch1/2/3.csv`), each containing repeated `order_id`s — confirming the `NOT EXISTS` check in `SP_LOAD_DIM_FACT` worked as intended under realistic incremental-load conditions.

---

## **20. Key Technical Skills Demonstrated**

1. Snowflake data warehousing (RAW, STAGING, CORE, REPORTING, AUDIT)
2. Snowpipe auto-ingestion and pipe lifecycle management
3. AWS S3 storage integration and IAM cross-account trust
4. Stored procedures with data validation and rejection logic
5. `MERGE`-based SCD Type 1 dimensional modeling
6. Incremental fact-table loading with duplicate prevention
7. Snowflake Task orchestration with dependency chains
8. Automated email alerting via `SYSTEM$SEND_EMAIL`
9. Least-privilege, role-based BI access
10. Pipeline observability via self-logging and `INFORMATION_SCHEMA`
11. Real-world debugging of pipe/stage lifecycle issues

---

## **21. How to Verify This Pipeline**

1. Run `Snowflake/ecomm_pipeline_setup.sql` once to build all infrastructure (update the IAM role ARN first).
2. Upload the source CSVs to the corresponding S3 folders (`customers/`, `products/`, `orders/`) — Snowpipe auto-ingests them.
3. Run `Snowflake/verify_pipeline.sql` to confirm row counts, rejected records, reporting output, task history, and grants.

---

## **22. Author**

**Vanshi Taneja**

Data Engineering / Analytics

**Technologies:** Snowflake · Snowpipe · SQL · Amazon S3 · AWS IAM · GitHub
