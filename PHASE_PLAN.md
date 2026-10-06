# NovaCart Order Analytics — Implementation Phase Plan

## 1. Roadmap Overview

The NovaCart hackathon implementation proceeds in 9 sequential, verified phases. Each phase requires successful execution, automated/manual verification against the actual dataset, and explicit stop gates before proceeding.

---

## 2. Detailed Phase Breakdown

### **PHASE 1: Discovery, Dataset Profiling & Architecture (CURRENT)**
- [x] Verify local environment (Python 3.10.11, AWS CLI).
- [x] Check AWS identity/credentials and establish security boundaries.
- [x] Copy and profile the full 8-file source dataset (`data/raw/`).
- [x] Validate all 13 known data-quality conditions with exact metrics.
- [x] Create `DATA_PROFILE.md` with treatment matrix.
- [x] Establish target Lakehouse architecture in `ARCHITECTURE.md`.
- [x] Define repository structure and configure `.gitignore`.
- [x] Produce `PHASE_PLAN.md`.
- **Gate:** Stop for user credential setup before cloud provisioning.

---

### **PHASE 2: AWS Foundation Provisioning & Iceberg Smoke Test**
- [ ] Create central config (`config/dev.yaml`).
- [ ] Create dedicated S3 bucket (`novacart-order-analytics-<suffix>`) with public access blocked and AES-256 encryption.
- [ ] Scaffold standard prefix layout (`landing/`, `bronze/`, `silver/`, `gold/`, `control/`, `athena-query-results/`).
- [ ] Create AWS Glue database `novacart`.
- [ ] Provision least-privilege IAM service role `NovaCartGlueRole`.
- [ ] Deploy and execute minimal PySpark Iceberg smoke test job (`novacart.environment_smoke_test`).
- [ ] Execute Athena validation query (`SELECT COUNT(*) = 3`).
- **Gate:** Structured Phase 2 Environment Status Report (All PASS).

---

### **PHASE 3: Bronze Ingestion Framework**
- [ ] Ingest Batch 1 source files into `bronze/` tier.
- [ ] Attach mandatory lineage metadata:
  - `batch_id`
  - `source_file`
  - `ingestion_timestamp`
- [ ] Archive raw structures without destructive transformation.
- **Gate:** Ingestion row counts match raw source records (orders: 1418, items: 1006, etc.).

---

### **PHASE 4: Silver Layer Cleansing & Quality Control**
- [ ] Implement multi-format timestamp parser to UTC ISO timestamp.
- [ ] Implement currency standardization (`UPPER(TRIM(currency))`).
- [ ] Standardize and cast `discount_pct` (handle nulls and strings).
- [ ] Implement composite key deduplication for order items `(order_id, line_no)`.
- [ ] Route unmatched payments and corrupted records to `silver/quarantine/`.
- [ ] Register Silver tables as Apache Iceberg in Glue Data Catalog.
- **Gate:** Clean Iceberg tables queryable; quarantine counts match expected violations.

---

### **PHASE 5: Incremental Processing & Idempotent MERGE (Batch 1 & Batch 2)**
- [ ] Execute Batch 1 initial load into Silver Iceberg tables.
- [ ] Process Batch 2 with Iceberg `MERGE INTO` logic:
  - Insert 450 new orders.
  - Update 132 modified orders based on `updated_at` precedence.
- [ ] **Demonstrate Idempotency:** Execute Batch 2 a second time. Verify that table snapshots and record counts are identical before and after rerun (`final_state_before_rerun == final_state_after_rerun`).
- **Gate:** TEST A, TEST B, TEST C, and TEST D verification passes.

---

### **PHASE 6: Customer Dimension SCD Type 2 Implementation**
- [ ] Ingest `customers_changes.csv` change stream.
- [ ] Model `silver.dim_customer_scd2` Iceberg table with:
  - `effective_from`
  - `effective_to`
  - `is_current`
- [ ] Assign seed timestamps to null `updated_at` records (`1970-01-01T00:00:00Z`).
- [ ] Demonstrate customer history for multi-version customers (e.g., `C0004`).
- **Gate:** TEST F verification passes.

---

### **PHASE 7: Gold Analytical Aggregations & Business KPIs**
- [ ] Build `gold.daily_revenue`: Daily revenue converted to USD using standardized `fx_rates`.
- [ ] Build `gold.revenue_by_category`: Aggregated sales, net returns, and margins by product category.
- [ ] Build `gold.customer_lifetime_value`: Net spend joined against current customer dimension.
- [ ] Build `gold.product_return_rates`: Return line ratios per product SKU.
- **Gate:** Validate KPI SQL queries via Amazon Athena.

---

### **PHASE 8: Operational Auditing, Monitoring & Control**
- [ ] Implement `control.batch_audit` logging for batch start/end, row counts, and statuses.
- [ ] Implement `control.dq_results` tracking rule failure metrics.
- [ ] Build pipeline runner script for orchestrated execution.
- **Gate:** Complete audit trail generated for Batch 1 and Batch 2 runs.

---

### **PHASE 9: Integration Verification, Documentation & Cleanup**
- [ ] Run full test suite (Tests A through G).
- [ ] Finalize `README.md` with complete architecture diagrams and runbooks.
- [ ] Produce `docs/environment_setup.md` and `docs/solution_design_reference.md`.
- [ ] Document clean teardown instructions for hackathon evaluators.
- **Gate:** Final Hackathon Submission Approval.
