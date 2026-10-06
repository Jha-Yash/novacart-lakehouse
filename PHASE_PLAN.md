# NovaCart Order Analytics — Implementation Phase Plan

## 1. Roadmap Overview

The NovaCart hackathon implementation proceeds in 9 sequential, verified phases. Every phase has been implemented, executed on live AWS services, and validated via Amazon Athena and automated pytest integration suites.

---

## 2. Detailed Phase Status Breakdown

### **PHASE 1: Discovery, Dataset Profiling & Architecture (COMPLETED)**
- [x] Verify local environment (Python 3.10.11, AWS CLI).
- [x] Check AWS identity/credentials and establish security boundaries.
- [x] Copy and profile the full 8-file source dataset (`data/raw/`).
- [x] Validate all 13 known data-quality conditions with exact metrics.
- [x] Create `DATA_PROFILE.md` with treatment matrix.
- [x] Establish target Lakehouse architecture in `ARCHITECTURE.md`.
- [x] Define repository structure and configure `.gitignore`.
- [x] Produce `PHASE_PLAN.md`.

---

### **PHASE 2: AWS Foundation Provisioning & Iceberg Smoke Test (COMPLETED)**
- [x] Create central config (`config/dev.yaml`).
- [x] Create dedicated S3 bucket (`novacart-order-analytics-125992594465-ap-southeast-2`) with public access blocked and AES-256 encryption.
- [x] Scaffold standard prefix layout (`landing/`, `bronze/`, `silver/`, `gold/`, `control/`, `athena-query-results/`, `iceberg-warehouse/`).
- [x] Create AWS Glue database `novacart`.
- [x] Provision least-privilege IAM service role `NovaCartGlueRole`.
- [x] Deploy and execute minimal PySpark Iceberg smoke test job (`novacart.environment_smoke_test`).
- [x] Execute Athena validation query (`SELECT COUNT(*) = 3`).

---

### **PHASE 3: Bronze Ingestion Framework (COMPLETED)**
- [x] Ingest Batch 1 source files into `bronze/` tier.
- [x] Attach mandatory lineage metadata:
  - `batch_id`
  - `source_file`
  - `ingestion_timestamp`
- [x] Archive raw structures without destructive transformation.
- [x] Validate row counts match raw source records (100% MATCH):
  - `bronze_orders`: 1,418 rows
  - `bronze_order_items`: 1,006 rows
  - `bronze_customers`: 194 rows
  - `bronze_fx_rates`: 84 rows
  - `bronze_products`: 40 rows
- [x] Register Bronze tables in AWS Glue Data Catalog (`novacart.bronze_*`).
- [x] Write operational audit metrics to `s3://.../control/batch_audit/`.

---

### **PHASE 4: Silver Layer Cleansing & Quality Control (COMPLETED)**
- [x] Implement multi-format timestamp parser normalizing `order_ts` into UTC (`order_ts_utc`).
- [x] Implement currency standardization (`UPPER(TRIM(currency))`) and country-based inference.
- [x] Standardize and cast `discount_pct` (handling nulls and string values).
- [x] Implement composite key deduplication for order items `(order_id, line_no)`.
- [x] Create and register Silver Apache Iceberg tables in Glue Data Catalog.
- [x] Build enriched `fact_order_line` table with normalized USD net and gross amounts.

---

### **PHASE 5: Incremental Processing & Idempotent MERGE (COMPLETED)**
- [x] Ingest Batch 2 into Bronze (1,545 orders, 1,007 items, 1,164 payments).
- [x] Process Batch 2 with Iceberg `MERGE INTO` logic:
  - Insert 450 new orders.
  - Update 132 modified orders based on `updated_at` precedence (total 945 unique orders).
- [x] Route 5 orphan payments (`NC-800001`–`NC-800005`) to `novacart.payments_quarantine`.
- [x] **Demonstrate Idempotency (TEST D):** Executed Batch 2 a second time. Verified table snapshots and record counts are identical before and after rerun (`final_state_before_rerun == final_state_after_rerun`).

---

### **PHASE 6: Customer Dimension SCD Type 2 Implementation (COMPLETED)**
- [x] Ingest `customers_changes.csv` change stream.
- [x] Model `novacart.dim_customer_scd2` Iceberg table with:
  - `effective_from`
  - `effective_to`
  - `is_current`
- [x] Assign seed timestamps to null `updated_at` records (`1970-01-01T00:00:00Z`).
- [x] Verify 130 active customer records (`is_current = true`) and 60 historical records.
- [x] Demonstrate customer history for `C0004` across 3 distinct chronological snapshots (**TEST F**).

---

### **PHASE 7: Gold Analytical Aggregations & Business KPIs (COMPLETED)**
- [x] Build `novacart.gold_daily_revenue`: Daily revenue converted to USD ($3,747,649.79 total across 30 active days).
- [x] Build `novacart.gold_revenue_by_category`: Aggregated units, gross revenue, net revenue, and return rate % by category.
- [x] Build `novacart.gold_customer_lifetime_value`: Net lifetime spend, orders, and first/last order dates joined with SCD2 customer dimension.
- [x] Build `novacart.gold_product_return_rates`: Return rates and root cause breakdown (`damaged`, `wrong_size`, `not_as_described`, `changed_mind`).
- [x] Validate all KPI SQL queries via Amazon Athena (**TEST G**).

---

### **PHASE 8: Operational Auditing, Monitoring & Orchestration (COMPLETED)**
- [x] Implement centralized audit logging in `s3://.../control/batch_audit/` across Bronze, Silver, and Gold.
- [x] Build automated Python orchestration runner [`src/orchestration/pipeline_runner.py`](file:///c:/Users/yashj/Desktop/hackathon/src/orchestration/pipeline_runner.py).

---

### **PHASE 9: Integration Verification, Documentation & Final Demo (COMPLETED)**
- [x] Run full automated pytest integration test suite (`tests/test_pipeline_e2e.py` — 9/9 tests passed).
- [x] Deliver curated Athena SQL query scripts in `sql/` (01 to 05).
- [x] Finalize `README.md` and `docs/environment_setup.md`.
- [x] All 7 critical test scenarios (TEST A through TEST G) 100% verified.
