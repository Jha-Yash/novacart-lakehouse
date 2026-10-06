# NovaCart Order Analytics — AWS Lakehouse

An enterprise-grade, cloud-native Order Analytics Lakehouse for NovaCart built on AWS using **Amazon S3**, **AWS Glue**, **Apache Iceberg**, and **Amazon Athena**.

---

## 1. Approved Architecture

```
[Source Datasets]
       ↓
[S3: landing/]
       ↓  (Glue PySpark Raw Ingestion + Batch Lineage Metadata)
[S3: bronze/]  (Immutable Raw Archive with audit columns)
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
├── README.md                 # Project overview and runbook
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
│   └── glue_smoke_test_job.json # Glue smoke test job definition
├── src/
│   ├── smoke_test/           # Environment validation Iceberg smoke test job
│   ├── ingestion/            # Landing to Bronze ingestion scripts
│   ├── bronze/               # Bronze storage definitions
│   ├── silver/               # Silver cleansing, deduplication, and SCD2
│   ├── gold/                 # Gold business KPI aggregations
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
| **Glue Smoke Job** | `novacart-iceberg-smoke-test` | `ap-southeast-2` | Active (Glue 4.0 + Iceberg) |
| **Iceberg Test Table**| `novacart.environment_smoke_test` | `ap-southeast-2` | Active (3 rows verified) |
| **Athena Query Output**| `s3://.../athena-query-results/` | `ap-southeast-2` | Verified (`SELECT COUNT(*) = 3`) |

---

## 4. Phase Status Summary

- [x] **Phase 1: Discovery, Dataset Profiling & Architecture** — Completed & Verified.
- [x] **Phase 2: AWS Foundation Provisioning & Iceberg Smoke Test** — Completed & Verified (All Gates PASS).
- [ ] **Phase 3: Bronze Ingestion Framework** — Next.
- [ ] **Phase 4: Silver Layer Cleansing & Quality Control**
- [ ] **Phase 5: Incremental Processing & Idempotent MERGE (Batch 1 & 2)**
- [ ] **Phase 6: Customer Dimension SCD Type 2 Implementation**
- [ ] **Phase 7: Gold Analytical Aggregations & Business KPIs**
- [ ] **Phase 8: Operational Auditing, Monitoring & Control**
- [ ] **Phase 9: End-to-End Integration Verification & Final Demo**
