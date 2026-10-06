# NovaCart Order Analytics — Target AWS Architecture

## 1. High-Level Architecture Overview

NovaCart Order Analytics follows a cloud-native AWS Medallion Lakehouse pattern leveraging **Apache Iceberg** tables registered in the **AWS Glue Data Catalog**, processed via **AWS Glue / PySpark**, and queried serverlessly through **Amazon Athena**.

```
[Source Files]
      ↓
[S3: landing/]
      ↓ (Glue PySpark Raw Ingestion + Metadata Tagging)
[S3: bronze/] (Parquet Raw Archive + batch_id + ingestion_ts)
      ↓ (Glue PySpark Cleansing, SCD2, FX Conversion, Iceberg MERGE)
[S3: silver/] (Apache Iceberg Tables in Glue Catalog)
      ↓ (Curated Aggregations & Analytical Rollups)
[S3: gold/]   (Apache Iceberg KPI Tables in Glue Catalog)
      ↓
[Amazon Athena] (Serverless SQL Queries & Business Analytics)
```

---

## 2. Core AWS Services & Technology Stack

| Layer / Concern | AWS Service / Technology | Rationale |
|---|---|---|
| **Cloud Storage** | Amazon S3 | Highly durable, scalable object store partitioned by medallion tiers. |
| **Table Format** | Apache Iceberg | ACID transactions, schema evolution, partition evolution, atomic `MERGE INTO` (upserts), time travel, and snapshot isolation. |
| **Metastore / Catalog** | AWS Glue Data Catalog | Central metadata repository natively integrated with Glue, Iceberg, and Athena. |
| **Processing Engine** | AWS Glue 4.0 (PySpark 3.3) | Managed serverless Spark runtime with native Iceberg support (`--datalake-formats iceberg`). |
| **Query Engine** | Amazon Athena Engine v3 | Interactive ANSI SQL query engine with native Iceberg table support. |
| **Security & IAM** | AWS IAM (`NovaCartGlueRole`) | Least-privilege IAM service role scoped to NovaCart S3 bucket and Glue Catalog. |
| **Audit & Control** | Iceberg / S3 Control Prefix | Centralized logging of batch runs, row counts, DQ violations, and pipeline watermarks. |

---

## 3. Storage Hierarchy (Amazon S3 Prefix Layout)

Inside the dedicated NovaCart S3 bucket (`novacart-order-analytics-<unique-suffix>`):

```
s3://novacart-order-analytics-<suffix>/
├── landing/                          # Source file staging
│   ├── batch_1/
│   │   ├── orders_batch_1.csv
│   │   ├── order_items_batch_1_json.txt
│   │   ├── customers_changes.csv
│   │   ├── fx_rates.csv
│   │   └── products.csv
│   └── batch_2/
│       ├── orders_batch_2.csv
│       ├── order_items_batch_2_jsonl.txt
│       └── payments_json.txt
├── bronze/                           # Raw immutable copy with audit metadata
│   ├── orders/
│   ├── order_items/
│   ├── customers/
│   ├── products/
│   ├── fx_rates/
│   └── payments/
├── silver/                           # Curated Apache Iceberg tables
│   ├── iceberg-warehouse/
│   │   ├── orders_current/
│   │   ├── order_items_current/
│   │   ├── dim_customer_scd2/
│   │   ├── products_current/
│   │   ├── fx_rates/
│   │   ├── payments_current/
│   │   └── fact_order_line/
│   └── quarantine/                   # Erroneous & unresolvable records
│       ├── orders/
│       ├── payments/
│       └── dq_violations/
├── gold/                             # Business aggregate Iceberg tables
│   ├── daily_revenue/
│   ├── revenue_by_category/
│   ├── customer_lifetime_value/
│   └── return_rate_by_product/
├── control/                          # Operational audit & watermark tracking
│   ├── watermark/
│   ├── batch_audit/
│   └── dq_results/
└── athena-query-results/             # Dedicated spill/output for Athena queries
```

---

## 4. Iceberg Catalog Configuration (AWS Glue Integration)

AWS Glue jobs run with native Iceberg Spark extensions and S3 file IO:
```text
--datalake-formats iceberg
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.glue_catalog.warehouse=s3://novacart-order-analytics-<suffix>/silver/iceberg-warehouse/
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

---

## 5. Security & Governance Baseline

1. **S3 Public Access Block:** Enforced at the bucket level (BlockPublicAcls, IgnorePublicAcls, BlockPublicPolicy, RestrictPublicBuckets).
2. **Encryption at Rest:** Enabled by default with server-side AWS-managed encryption (`AES256` / SSE-S3).
3. **IAM Least Privilege:**
   - S3 permissions restricted strictly to `arn:aws:s3:::novacart-order-analytics-*/*`.
   - Glue permissions restricted to `novacart` database and associated tables.
   - CloudWatch Logs permissions limited to `/aws-glue/jobs/*`.

---

## 6. Audit & Data Quality Architecture

Each processing phase logs into `control/batch_audit` with:
- `batch_id`
- `pipeline_stage` (`BRONZE_INGESTION`, `SILVER_CURATION`, `GOLD_AGGREGATION`)
- `start_time_utc`, `end_time_utc`
- `input_records`, `output_records`, `quarantined_records`
- `status` (`SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`)
- `error_message`
