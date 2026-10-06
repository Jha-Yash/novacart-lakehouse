# NovaCart Source Dataset Profile & Data Quality Assessment

## 1. Executive Summary
This document provides a comprehensive, empirical profile of the actual NovaCart Order Analytics source datasets, validating known data-quality conditions and establishing authoritative processing rules for Bronze ingestion, Silver curation, and Gold aggregation.

---

## 2. Dataset Inventory & Schema Profile

### 2.1 `customers_changes.csv`
- **File Type:** CSV (Comma-separated)
- **Total Records:** 194
- **Distinct `customer_id`:** 130
- **Customers with History (Multiple Rows):** 49
- **Schema:**
  - `customer_id` (String): e.g., `C0004`, `C0009`
  - `full_name` (String): e.g., `Zara Tan`, `Liam Wilson`
  - `email` (String): e.g., `zara.tan4.work@example.org`
  - `tier` (String): `Bronze`, `Silver`, `Gold`, `Platinum`
  - `country` (String, 2-letter ISO): `US`, `IN`, `DE`, `GB`, `SG`
  - `updated_at` (String, ISO Timestamp / Null): Timestamp of the change
- **Key Observations:**
  - **Change / History Semantics:** This is an SCD Type 2 change stream rather than a static snapshot. For example, `C0004` appears 3 times (`2026-08-18` in IN, `2026-09-13` in IN with work email, `2026-09-14` in DE).
  - **Null Timestamps:** Exactly 5 records have a null `updated_at`. These represent initial baseline state (epoch / seed registrations).

---

### 2.2 `fx_rates.csv`
- **File Type:** CSV
- **Total Records:** 84
- **Date Range:** `2026-09-01` to `2026-09-30`
- **Schema:**
  - `rate_date` (String, YYYY-MM-DD)
  - `currency` (String, ISO 3-letter): `EUR`, `GBP`, `INR`, `SGD`
  - `rate_to_usd` (Float/Decimal): Rate to multiply local currency by to convert to USD
- **Key Observations:**
  - USD is the base currency (rate = 1.0).
  - All foreign currencies (`EUR`, `GBP`, `INR`, `SGD`) have daily closing rates across the entire September 2026 period.

---

### 2.3 `products.csv`
- **File Type:** CSV
- **Total Records:** 40
- **Distinct `product_id`:** 40 (`P001` through `P040`)
- **Schema:**
  - `product_id` (String): Unique identifier
  - `sku` (String): Stock keeping unit, e.g., `NC-ELE-001`
  - `product_name` (String): e.g., `Wireless Earbuds`
  - `category` (String): `Electronics` (10), `Home` (8), `Fashion` (8), `Beauty` (6), `Sports` (5), `Books` (3)
  - `list_price_usd` (Float): Catalog list price in USD

---

### 2.4 `orders_batch_1.csv` & `orders_batch_2.csv`
- **File Type:** CSV
- **Schema:**
  - `order_id` (String): e.g., `NC-100354`
  - `customer_id` (String): e.g., `C0080`
  - `order_ts` (String): Mixed timestamp formats
  - `status` (String): `placed`, `paid`, `shipped`, `delivered`, `cancelled`
  - `currency` (String): Mixed case, occasionally null
  - `shipping_country` (String): Country code
  - `updated_at` (String, ISO Timestamp): Last state change timestamp
  - `promo_code` (String / Null): e.g., `FESTIVE15`, `VIP20`, `WELCOME10`, `FREESHIP`
- **Batch Characteristics:**
  - **Batch 1:** 1,418 rows (495 unique `order_id` values, representing lifecycle state events)
  - **Batch 2:** 1,545 rows (597 unique `order_id` values)
  - **Overlapping Orders:** Exactly 147 `order_id` values exist in both Batch 1 and Batch 2.
  - **Incremental Updates:** 132 of the 147 overlapping orders contain status and/or `updated_at` changes between Batch 1 and Batch 2 (e.g. `NC-100390` updated from `placed` on 2026-09-12 to `delivered` on 2026-09-16).
  - **New Orders in Batch 2:** Exactly 450 new `order_id` values introduced in Batch 2.

---

### 2.5 `order_items_batch_1_json.txt` & `order_items_batch_2_jsonl.txt`
- **File Type:** JSON Lines (JSONL)
- **Batch 1 Records:** 1,006
- **Batch 2 Records:** 1,007
- **Schema:**
  - `order_id` (String): Order identifier
  - `line_no` (Integer): Line item position within the order
  - `product_id` (String): Product identifier
  - `qty` (Integer): Positive for sales, negative for returns
  - `unit_price` (Float/Decimal): Item sale price in local currency
  - `discount_pct` (Int/Float/String/Null): Discount percentage
  - `line_type` (String): `sale` (1,923 lines) vs `return` (90 lines)
  - `attributes` (JSON Object / Null): Nested key-values (e.g., `color`, `size`, `warranty_months`, `volume_ml`, `gift_wrap`, `reason`)
- **Key Observations:**
  - **Duplicate Composite Keys:** 25 duplicate `(order_id, line_no)` pairs in Batch 1, 25 in Batch 2, and 62 across the combined dataset.
  - **Negative Quantities:** Exactly 90 records have negative quantity (`qty < 0`), and 100% of them have `line_type = 'return'`.
  - **Type Inconsistencies:** `discount_pct` is represented as integers (1,672), floats (66), nulls (251), and string literals like `"10"`, `"12.5"`, `"20"` (24 lines).

---

### 2.6 `payments_json.txt`
- **File Type:** JSON Array
- **Total Records:** 1,164
- **Schema:**
  - `payment_id` (String): Unique transaction key, e.g., `PAY-500747`
  - `order_id` (String): Associated order identifier
  - `method` (String): `card` (566), `paypal` (206), `upi` (183), `netbanking` (90), `wallet` (76), `cod` (43)
  - `amount` (Float/Decimal): Payment amount
  - `currency` (String): Currency code
  - `status` (String): `success` (858), `failed` (179), `refunded` (127)
  - `paid_at` (String, ISO Timestamp): Payment execution timestamp
  - `gateway_ref` (String): Gateway reference identifier

---

## 3. Data-Quality Conditions & Handling Strategy Matrix

| Quality Condition | Verified Finding | Recommended Decision | Handling Rule in Silver Layer |
|---|---|---|---|
| **Multiple `order_ts` Formats** | ISO UTC `Z` (1,106), ISO `+05:30` (940), `DD/MM/YYYY HH:MM` (917) | **Standardize / Fix** | Parse all three format variations and normalize to standardized UTC timestamp (`order_ts_utc`). |
| **Mixed-case Currency** | Lowercase `eur`, `inr`, `usd`, `sgd`, `gbp` alongside uppercase | **Standardize / Fix** | Standardize via `UPPER(TRIM(currency))`. |
| **Null Currency in Orders** | 194 order records have null currency | **Standardize / Fix** | Infer currency from matching customer country or matching payment record; if unresolvable, flag `currency_inferred=False` and quarantine. |
| **Null `promo_code`** | 1,227 records have null promo codes | **Accept as Valid** | Null promo code represents a standard order with no promotional discount applied. Coerce to `'NONE'` or leave as null flag. |
| **`customers_changes` Duplicate IDs** | 49 customer IDs have 2–3 rows | **Standardize / Fix (SCD2)** | Model as SCD Type 2 dimension (`dim_customer_scd2`) with `effective_from`, `effective_to`, `is_current`. |
| **Null `updated_at` in Customers** | 5 customer records have null `updated_at` | **Standardize / Fix** | Treat null `updated_at` as the initial baseline version (effective from `1970-01-01T00:00:00Z`). |
| **Duplicate `(order_id, line_no)`** | 25 duplicates per batch, 62 across dataset | **Standardize / Deduplicate** | Deduplicate using deterministic window: order by ingestion timestamp and status/action to retain the latest authoritative line. |
| **Order Items Return Lines** | 90 return lines with negative `qty` | **Accept with Quality Flag** | Retain line items with `line_type = 'return'`, ensure `qty` sign is respected in net revenue calculations. |
| **`discount_pct` Types & Nulls** | 251 nulls, 24 strings (`"10"`, `"12.5"`), 1,738 numeric | **Standardize / Fix** | Cast strings to float, replace nulls with `0.0`, enforce `discount_pct BETWEEN 0.0 AND 100.0`. |
| **Unknown `customer_id` in Orders** | 6 customer IDs (`C0901`–`C0906`) not in customer history | **Accept with Quality Flag** | Do NOT delete valid orders; associate with a placeholder/unknown customer dimension record (`customer_id = 'UNKNOWN'` or `C090x` with `is_orphan=True`). |
| **Unknown `product_id` in Order Items** | 4 product IDs (`P101`, `P102`, `P103`, `P105`) not in `products.csv` | **Accept with Quality Flag** | Route line items with unknown product catalog metadata to an `UNKNOWN_PRODUCT` category or flag `catalog_matched=False`. |
| **Orphan Payments** | 5 payments reference order IDs `NC-800001` to `NC-800005` | **Quarantine / Flag** | Route unmatched payment transactions to `silver/quarantine/payments_orphan` with reason code `UNMATCHED_ORDER_ID`. |
| **Batch 1 & Batch 2 Overlaps** | 147 overlapping orders (132 updated status/timestamps) | **Incremental MERGE** | Apply Iceberg `MERGE INTO` matching on `order_id` when `source.updated_at >= target.updated_at` to ensure idempotency. |

---

## 4. Next Phase Readiness
- **Data Source of Truth:** All 8 files are physically placed in `data/raw/` and verified.
- **Rules Defined:** All 13 known quality conditions have unambiguous, deterministic handling rules.
- **Environment Dependency:** Phase 2 (AWS provisioning) is blocked until AWS credentials are provided.
