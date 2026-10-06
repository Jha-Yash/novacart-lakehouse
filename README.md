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
├── src/
│   ├── ingestion/            # Landing to Bronze ingestion scripts
│   ├── bronze/               # Bronze storage definitions
│   ├── silver/               # Silver cleansing, deduplication, and SCD2
│   ├── gold/                 # Gold business KPI aggregations
│   ├── quality/              # Data quality validation and quarantine filters
│   └── utils/                # Logging, audit, and helper modules
├── sql/                      # Athena analytical queries & DDL
├── tests/                    # Unit and integration test suites
├── config/                   # Non-secret environment configurations
└── docs/                     # Detailed architectural and operational guides
```

---

## 3. Dataset Summary

The NovaCart source dataset contains 8 verified files in `data/raw/`:

| File | Rows / Records | Format | Primary Role |
|---|---|---|---|
| `customers_changes.csv` | 194 rows | CSV | Customer change stream for SCD Type 2 dimension |
| `fx_rates.csv` | 84 rows | CSV | Daily exchange rates to USD for EUR, GBP, INR, SGD |
| `products.csv` | 40 rows | CSV | Product catalog metadata across 6 categories |
| `orders_batch_1.csv` | 1,418 rows | CSV | Initial order lifecycle event batch (495 unique orders) |
| `orders_batch_2.csv` | 1,545 rows | CSV | Incremental order batch (450 new, 132 modified orders) |
| `order_items_batch_1_json.txt` | 1,006 lines | JSONL | Order line items (Batch 1) with discounts & attributes |
| `order_items_batch_2_jsonl.txt` | 1,007 lines | JSONL | Order line items (Batch 2) with returns and updates |
| `payments_json.txt` | 1,164 records | JSON | Transaction records with status, method, and gateway refs |

---

## 4. Phase 1 Status: COMPLETED

Phase 1 (Discovery, Data Profiling, and Architecture) is complete:
- [x] Local environment verified (Python 3.10.11, AWS CLI 1.46.1).
- [x] Source files inspected and copied to `data/raw/`.
- [x] All 13 data quality anomalies empirically validated with exact metrics.
- [x] Quality handling decision matrix established in [`DATA_PROFILE.md`](file:///c:/Users/yashj/Desktop/hackathon/DATA_PROFILE.md).
- [x] Lakehouse architecture specified in [`ARCHITECTURE.md`](file:///c:/Users/yashj/Desktop/hackathon/ARCHITECTURE.md).
- [x] Implementation roadmap detailed in [`PHASE_PLAN.md`](file:///c:/Users/yashj/Desktop/hackathon/PHASE_PLAN.md).
- [x] Security controls configured via [`.gitignore`](file:///c:/Users/yashj/Desktop/hackathon/.gitignore).

---

## 5. Prerequisites for Phase 2 Provisioning

To proceed with **Phase 2** (provisioning the NovaCart S3 bucket, Glue database, Glue IAM role, and executing the Iceberg smoke test), configure AWS credentials on your local machine:

```powershell
aws configure
```
Provide:
- **AWS Access Key ID:** `<your-access-key-id>`
- **AWS Secret Access Key:** `<your-secret-access-key>`
- **Default region name:** e.g., `us-east-1`
- **Default output format:** `json`
