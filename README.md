# NovaCart Order Analytics — AWS Lakehouse

An enterprise-grade, cloud-native Order Analytics Lakehouse for NovaCart built on AWS using **Amazon S3**, **AWS Glue**, **Apache Iceberg**, and **Amazon Athena**.

---

## 1. Approved Architecture

```
[Source Datasets]
       ↓
[S3: landing/]
       ↓  (Glue PySpark Raw Ingestion + Batch Lineage Metadata)
[S3: bronze/]  (Immutable Raw Archive with audit columns: batch_id, source_file, ingestion_ts)
       ↓  (Glue PySpark Cleansing, SCD2, Currency Normalization, Iceberg MERGE)
[S3: silver/]  (Curated Apache Iceberg Tables registered in Glue Catalog)
       ↓  (Business-ready Aggregations & Analytics)
[S3: gold/]    (Gold Iceberg KPI Tables)
       ↓
[Amazon Athena] (Serverless ANSI SQL Query Engine)
```

---

## 2. Project Directory Structure

```text
hackathon/
├── README.md                 # Master project documentation & guide
├── DATA_PROFILE.md           # Dataset profile, row counts, and DQ treatment matrix
├── ARCHITECTURE.md           # AWS Lakehouse architecture specification
├── PHASE_PLAN.md             # 9-Phase execution plan and validation gates
├── requirements.txt          # Python dependencies
├── .gitignore                # Security and environment ignore rules
├── data/
│   └── raw/                  # Source datasets (customers, orders, items, fx, etc.)
├── scripts/
│   └── profile_dataset.py    # Empirical dataset profiling utility
├── infrastructure/           # CloudFormation templates and job definitions
│   ├── foundation.yaml       # S3 bucket, Glue DB, IAM Role
│   ├── glue_smoke_test_job.json # Glue smoke test job definition
│   ├── glue_bronze_job.json  # Bronze ingestion Glue job definition
│   ├── glue_silver_job.json  # Silver curation Glue job definition
│   ├── glue_scd2_job.json    # Customer SCD2 Glue job definition
│   └── glue_gold_job.json    # Gold analytics Glue job definition
├── src/
│   ├── smoke_test/           # Environment validation Iceberg smoke test job
│   ├── bronze/               # Bronze ingestion pipeline (Landing to Bronze)
│   ├── silver/               # Silver cleansing, deduplication, and SCD2
│   ├── gold/                 # Gold business KPI aggregations
│   ├── orchestration/        # Unified Python pipeline runner
│   └── utils/                # Helper utilities and audit logging
├── sql/                      # Curated Athena analytical SQL queries
│   ├── 01_daily_revenue.sql
│   ├── 02_revenue_by_category.sql
│   ├── 03_customer_lifetime_value.sql
│   ├── 04_product_return_rates.sql
│   └── 05_scd2_customer_history.sql
├── tests/                    # Automated test suites
│   ├── verify_idempotency.py # Idempotency verification script
│   ├── verify_gold_kpis.py   # Gold KPI verification script
│   └── test_pipeline_e2e.py  # Full pytest integration test suite (9/9 passed)
├── config/                   # Central non-secret environment configurations
└── docs/                     # Detailed architectural and operational guides
    └── environment_setup.md  # Complete environment and validation report
```

---

## 3. Provisioned AWS Resources

| Resource | Resource Name / Identifier | Region | Status |
|---|---|---|---|
| **S3 Bucket** | `novacart-order-analytics-125992594465-ap-southeast-2` | `ap-southeast-2` | Active (AES-256, Public Blocked) |
| **Glue Database** | `novacart` | `ap-southeast-2` | Active |
| **Glue IAM Role** | `NovaCartGlueRole` | Global / IAM | Active (Least privilege) |
| **Bronze Tables** | `bronze_orders` (2,963 rows)<br>`bronze_order_items` (2,013 rows)<br>`bronze_customers` (194 rows)<br>`bronze_fx_rates` (84 rows)<br>`bronze_products` (40 rows)<br>`bronze_payments` (1,164 rows) | `ap-southeast-2` | Active & Verified |
| **Silver Tables** | `orders_current` (945 unique orders)<br>`order_items_current` (1,951 unique items)<br>`products_current` (40 products)<br>`fx_rates_current` (84 rates + USD base)<br>`payments_current` (1,159 valid payments)<br>`payments_quarantine` (5 orphan payments)<br>`dim_customer_scd2` (130 active, 60 history)<br>`fact_order_line` (1,933 enriched facts) | `ap-southeast-2` | Active (Apache Iceberg v2) |
| **Gold Tables** | `gold_daily_revenue` (30 active days)<br>`gold_revenue_by_category` (6 categories + uncataloged)<br>`gold_customer_lifetime_value` (Top spenders)<br>`gold_product_return_rates` (44 products + reasons) | `ap-southeast-2` | Active (Apache Iceberg v2) |
| **Athena Query Output**| `s3://.../athena-query-results/` | `ap-southeast-2` | Verified via automated test suite |

---

## 4. Key Business KPI Results (Queried via Athena)

1. **Total September 2026 Net Revenue:** **\$3,747,649.79 USD** across 30 active days.
2. **Top Category by Net Revenue:** **Electronics** (\$2,941,711.89 USD, 1,140 units sold, 42 returns, 3.47% return rate).
3. **Top Spender:** **C0003 (Jonas Schmidt, Bronze)** (\$584,648.99 USD net spend, 45 orders).
4. **Top Returned Product:** **Wireless Earbuds** (9 units returned, 7.26% return rate; root causes: 4 wrong size, 3 changed mind, 1 damaged, 1 not as described).
5. **Data Quality Quarantine:** 5 orphan payments (`NC-800001`–`NC-800005`) successfully quarantined.

---

## 5. Automated Test Suite Verification

Run the complete test suite:
```powershell
python -m pytest tests/test_pipeline_e2e.py -v
```

**Results:**
```text
tests/test_pipeline_e2e.py::test_s3_bucket_and_encryption PASSED         [ 11%]
tests/test_pipeline_e2e.py::test_s3_public_access_block PASSED           [ 22%]
tests/test_pipeline_e2e.py::test_glue_database PASSED                    [ 33%]
tests/test_pipeline_e2e.py::test_bronze_table_counts PASSED              [ 44%]
tests/test_pipeline_e2e.py::test_silver_orders_current PASSED            [ 55%]
tests/test_pipeline_e2e.py::test_silver_order_items_current PASSED       [ 66%]
tests/test_pipeline_e2e.py::test_silver_payments PASSED                  [ 77%]
tests/test_pipeline_e2e.py::test_silver_scd2_customers PASSED            [ 88%]
tests/test_pipeline_e2e.py::test_gold_kpi_tables PASSED                  [100%]

============================= 9 passed in 59.17s ==============================
```

---

## 6. How to Run the Pipeline

To execute the entire pipeline end-to-end:
```powershell
python src/orchestration/pipeline_runner.py
```
