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
│   └── glue_bronze_job.json  # Bronze ingestion Glue job definition
├── src/
│   ├── smoke_test/           # Environment validation Iceberg smoke test job
│   ├── bronze/               # Bronze ingestion pipeline (Landing to Bronze)
│   ├── silver/               # Silver cleansing, deduplication, and SCD2 (Phase 4)
│   ├── gold/                 # Gold business KPI aggregations (Phase 7)
│   ├── quality/              # Data quality validation and quarantine filters
│   └── utils/                # Logging, audit, and helper modules
├── sql/                      # Athena analytical queries & DDL
├── tests/                    # Unit and integration test suites
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
| **Bronze Tables** | `bronze_orders` (1,418 rows)<br>`bronze_order_items` (1,006 rows)<br>`bronze_customers` (194 rows)<br>`bronze_fx_rates` (84 rows)<br>`bronze_products` (40 rows) | `ap-southeast-2` | Active & Verified via Athena |
| **Iceberg Test Table**| `novacart.environment_smoke_test` | `ap-southeast-2` | Active (3 rows verified) |
| **Athena Query Output**| `s3://.../athena-query-results/` | `ap-southeast-2` | Verified |

---

## 4. Phase Status Summary

- [x] **Phase 1: Discovery, Dataset Profiling & Architecture** — Completed & Verified.
- [x] **Phase 2: AWS Foundation Provisioning & Iceberg Smoke Test** — Completed & Verified.
- [x] **Phase 3: Bronze Ingestion Framework (Batch 1)** — Completed & Verified (100% row count match).
- [ ] **Phase 4: Silver Layer Cleansing & Quality Control** — Next.
- [ ] **Phase 5: Incremental Processing & Idempotent MERGE (Batch 1 & 2)**
- [ ] **Phase 6: Customer Dimension SCD Type 2 Implementation**
- [ ] **Phase 7: Gold Analytical Aggregations & Business KPIs**
- [ ] **Phase 8: Operational Auditing, Monitoring & Control**
- [ ] **Phase 9: End-to-End Integration Verification & Final Demo**
