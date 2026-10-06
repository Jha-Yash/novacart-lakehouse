# NovaCart Order Analytics — Environment Setup & Infrastructure Baseline

## 1. Overview
This document details the provisioned infrastructure, security baselines, IAM configurations, Apache Iceberg catalog integration, and validation results for the NovaCart Order Analytics Lakehouse on AWS.

---

## 2. Environment Details

- **Cloud Provider:** AWS
- **AWS Account ID:** `125992594465`
- **Authorized Region:** `ap-southeast-2` (Sydney)
- **Environment:** `dev`
- **Primary Lakehouse Bucket:** `novacart-order-analytics-125992594465-ap-southeast-2`
- **Glue Catalog Database:** `novacart`
- **Glue IAM Service Role:** `NovaCartGlueRole` (`arn:aws:iam::125992594465:role/NovaCartGlueRole`)
- **Glue Engine Runtime:** AWS Glue 4.0 (Apache Spark 3.3.0, Scala 2.12, Python 3.10)
- **Query Engine:** Amazon Athena Engine Version 3 (supports Apache Iceberg)

---

## 3. S3 Storage Architecture & Security Baseline

### 3.1 Security Controls
1. **Public Access Block:** Enforced across all four axes:
   - `BlockPublicAcls = true`
   - `IgnorePublicAcls = true`
   - `BlockPublicPolicy = true`
   - `RestrictPublicBuckets = true`
2. **Encryption at Rest:** Server-side encryption enabled by default:
   - `SSEAlgorithm = AES256` (SSE-S3)
3. **Dedicated Prefix Layout:**
   - `landing/`: Raw staging for source batch files (`batch_1/`, `batch_2/`)
   - `bronze/`: Immutable ingested source data with lineage metadata (`orders/`, `order_items/`, `customers/`, `products/`, `fx_rates/`, `payments/`)
   - `silver/`: Curated Apache Iceberg tables (`orders_current/`, `order_items_current/`, `dim_customer_scd2/`, `products_current/`, `fx_rates/`, `payments_current/`, `fact_order_line/`, `quarantine/`)
   - `gold/`: High-performance analytical Iceberg tables (`daily_revenue/`, `revenue_by_category/`)
   - `control/`: Pipeline orchestration watermarks, audit logs, and data quality results (`watermark/`, `batch_audit/`, `dq_results/`)
   - `athena-query-results/`: Dedicated spill location for Athena query results
   - `iceberg-warehouse/`: Underlying warehouse location for Iceberg metadata and data parquet files

---

## 4. Apache Iceberg Catalog Configuration

The project utilizes the **AWS Glue Data Catalog** as the central Apache Iceberg catalog. The Spark configuration parameters supplied to AWS Glue jobs are:

```properties
--datalake-formats=iceberg
--enable-glue-datacatalog=true
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.warehouse=s3://novacart-order-analytics-125992594465-ap-southeast-2/iceberg-warehouse/
spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

---

## 5. IAM Permissions Architecture

The `NovaCartGlueRole` was created following the principle of least privilege:
- **Trust Relationship:** `glue.amazonaws.com`
- **Managed Policy:** `arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole` (CloudWatch Logging and basic Glue service hooks)
- **Inline Policy (`NovaCartS3Access`):**
  - Scoped to `arn:aws:s3:::novacart-order-analytics-125992594465-ap-southeast-2` and `/*`
  - Actions: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`, `s3:GetBucketLocation`
- **Inline Policy (`NovaCartGlueCatalogAccess`):**
  - Scoped to database `novacart` and all tables within it
  - Actions: Full DDL and DML operations on Glue catalog database and tables

---

## 6. Smoke-Test Procedure & Validation Results

### 6.1 AWS Glue Smoke Test Execution
- **Job Name:** `novacart-iceberg-smoke-test`
- **Run ID:** `jr_22d4becc0da6c7f00898b70743ae67fb3a5f28a1a34fdb18145f7ef4c695407e`
- **Execution Time:** 69 seconds
- **Job Status:** `SUCCEEDED`
- **Target Table:** `novacart.environment_smoke_test`
- **Verification:** Spark SQL wrote 3 rows and verified row count via internal assertion `assert row_count == 3`.

### 6.2 Data Catalog & S3 Storage Verification
- **Catalog Registration:**
  - Table: `novacart.environment_smoke_test`
  - Type: `EXTERNAL_TABLE`
  - Table Type: `ICEBERG`
  - Metadata Location: `s3://novacart-order-analytics-125992594465-ap-southeast-2/iceberg-warehouse/novacart.db/environment_smoke_test/metadata/00001-71f01201-2ee7-4380-8fa0-0a305c742351.metadata.json`
- **Underlying Files Created:**
  - 3 Parquet data files under `iceberg-warehouse/novacart.db/environment_smoke_test/data/`
  - Metadata JSON files and Avro manifest list files under `iceberg-warehouse/novacart.db/environment_smoke_test/metadata/`

### 6.3 Amazon Athena Query Results
- **Query 1:** `SELECT * FROM novacart.environment_smoke_test ORDER BY id;`
  - Result:
    ```text
    ['1', 'NovaCart Alpha', '2026-10-06 10:00:00.000000 UTC']
    ['2', 'NovaCart Beta', '2026-10-06 11:00:00.000000 UTC']
    ['3', 'NovaCart Gamma', '2026-10-06 12:00:00.000000 UTC']
    ```
- **Query 2:** `SELECT COUNT(*) AS total_count FROM novacart.environment_smoke_test;`
  - Result: **`3`**
- **Athena Status:** **PASS** (100% successful serverless query execution over native Iceberg table).
