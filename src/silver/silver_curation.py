import sys
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lit, when, upper, lower, trim, coalesce, to_utc_timestamp,
    to_timestamp, date_format, to_date, row_number, current_timestamp
)
from pyspark.sql.window import Window

# Resolve arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'BATCH_ID'])
bucket = args['BUCKET_NAME']
batch_id = args['BATCH_ID']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

start_time = datetime.now(timezone.utc).isoformat()
print(f">>> [Silver Curation] Starting Silver pipeline for {batch_id} at {start_time}")

catalog = "glue_catalog"
database = "novacart"

# ==============================================================================
# 1. CURATE PRODUCTS (Batch 1 or whenever present)
# ==============================================================================
products_table = f"{catalog}.{database}.products_current"
if batch_id == "batch_1":
    print("\n>>> 1. Curating products into Iceberg table: novacart.products_current...")
    bronze_products = spark.read.parquet(f"s3://{bucket}/bronze/products/")
    products_curated = bronze_products.select(
        trim(col("product_id")).alias("product_id"),
        trim(col("sku")).alias("sku"),
        trim(col("product_name")).alias("product_name"),
        trim(col("category")).alias("category"),
        col("list_price_usd").cast("double").alias("list_price_usd"),
        col("batch_id"),
        col("ingestion_timestamp")
    ).dropDuplicates(["product_id"])

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {products_table} (
        product_id STRING,
        sku STRING,
        product_name STRING,
        category STRING,
        list_price_usd DOUBLE,
        batch_id STRING,
        ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
    """)

    products_curated.createOrReplaceTempView("src_products")
    spark.sql(f"""
    MERGE INTO {products_table} t
    USING src_products s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print(f">>> products_current row count: {spark.table(products_table).count()}")

# ==============================================================================
# 2. CURATE FX_RATES (Batch 1 or whenever present)
# ==============================================================================
fx_table = f"{catalog}.{database}.fx_rates_current"
if batch_id == "batch_1":
    print("\n>>> 2. Curating fx_rates into Iceberg table: novacart.fx_rates_current...")
    bronze_fx = spark.read.parquet(f"s3://{bucket}/bronze/fx_rates/")
    fx_curated = bronze_fx.select(
        to_date(trim(col("rate_date")), "yyyy-MM-dd").alias("rate_date"),
        upper(trim(col("currency"))).alias("currency"),
        col("rate_to_usd").cast("double").alias("rate_to_usd"),
        col("batch_id"),
        col("ingestion_timestamp")
    ).dropDuplicates(["rate_date", "currency"])

    distinct_dates = fx_curated.select("rate_date").distinct()
    usd_rates = distinct_dates.select(
        col("rate_date"),
        lit("USD").alias("currency"),
        lit(1.0).alias("rate_to_usd"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("ingestion_timestamp")
    )
    all_fx_curated = fx_curated.unionByName(usd_rates).dropDuplicates(["rate_date", "currency"])

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {fx_table} (
        rate_date DATE,
        currency STRING,
        rate_to_usd DOUBLE,
        batch_id STRING,
        ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
    """)

    all_fx_curated.createOrReplaceTempView("src_fx")
    spark.sql(f"""
    MERGE INTO {fx_table} t
    USING src_fx s
    ON t.rate_date = s.rate_date AND t.currency = s.currency
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print(f">>> fx_rates_current row count: {spark.table(fx_table).count()}")

# ==============================================================================
# 3. CURATE ORDERS (IDEMPOTENT MERGE)
# ==============================================================================
print(f"\n>>> 3. Curating orders for {batch_id} into Iceberg table: novacart.orders_current...")
bronze_orders = spark.read.parquet(f"s3://{bucket}/bronze/orders/batch_id={batch_id}/")

ts_expr = when(col("order_ts").like("%T%Z"), to_timestamp(col("order_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .when(col("order_ts").like("%+%"), to_utc_timestamp(to_timestamp(col("order_ts"), "yyyy-MM-dd'T'HH:mm:ssXXX"), "UTC")) \
    .when(col("order_ts").like("%/%"), to_timestamp(col("order_ts"), "dd/MM/yyyy HH:mm")) \
    .otherwise(to_timestamp(col("order_ts")))

updated_ts_expr = when(col("updated_at").like("%T%Z"), to_timestamp(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .when(col("updated_at").like("%+%"), to_utc_timestamp(to_timestamp(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ssXXX"), "UTC")) \
    .otherwise(to_timestamp(col("updated_at")))

curr_clean = upper(trim(col("currency")))
currency_expr = when(curr_clean.isNotNull() & (curr_clean != ""), curr_clean) \
    .when(col("shipping_country") == "US", lit("USD")) \
    .when(col("shipping_country") == "IN", lit("INR")) \
    .when(col("shipping_country") == "GB", lit("GBP")) \
    .when(col("shipping_country") == "DE", lit("EUR")) \
    .when(col("shipping_country") == "SG", lit("SGD")) \
    .otherwise(lit("UNKNOWN"))

orders_transformed = bronze_orders.select(
    trim(col("order_id")).alias("order_id"),
    trim(col("customer_id")).alias("customer_id"),
    ts_expr.alias("order_ts_utc"),
    to_date(ts_expr).alias("order_date"),
    lower(trim(col("status"))).alias("status"),
    currency_expr.alias("currency"),
    upper(trim(col("shipping_country"))).alias("shipping_country"),
    updated_ts_expr.alias("updated_at_utc"),
    coalesce(trim(col("promo_code")), lit("NONE")).alias("promo_code"),
    when(trim(col("customer_id")).isin(["C0901", "C0902", "C0903", "C0904", "C0905", "C0906"]), lit(True)).otherwise(lit(False)).alias("is_orphan_customer"),
    col("batch_id"),
    col("ingestion_timestamp")
)

w_order = Window.partitionBy("order_id").orderBy(col("updated_at_utc").desc_nulls_last())
orders_deduped = orders_transformed.withColumn("rn", row_number().over(w_order)) \
    .filter(col("rn") == 1) \
    .drop("rn")

orders_table = f"{catalog}.{database}.orders_current"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {orders_table} (
    order_id STRING,
    customer_id STRING,
    order_ts_utc TIMESTAMP,
    order_date DATE,
    status STRING,
    currency STRING,
    shipping_country STRING,
    updated_at_utc TIMESTAMP,
    promo_code STRING,
    is_orphan_customer BOOLEAN,
    batch_id STRING,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES ('format-version' = '2')
""")

orders_deduped.createOrReplaceTempView("src_orders")
spark.sql(f"""
MERGE INTO {orders_table} t
USING src_orders s
ON t.order_id = s.order_id
WHEN MATCHED AND (s.updated_at_utc >= t.updated_at_utc OR t.updated_at_utc IS NULL) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
orders_current_count = spark.table(orders_table).count()
print(f">>> orders_current row count after {batch_id}: {orders_current_count}")

# ==============================================================================
# 4. CURATE ORDER_ITEMS (IDEMPOTENT MERGE)
# ==============================================================================
print(f"\n>>> 4. Curating order_items for {batch_id} into Iceberg table: novacart.order_items_current...")
bronze_items = spark.read.parquet(f"s3://{bucket}/bronze/order_items/batch_id={batch_id}/")

discount_expr = when(col("discount_pct").isNull(), lit(0.0)) \
    .otherwise(col("discount_pct").cast("double"))

items_transformed = bronze_items.select(
    trim(col("order_id")).alias("order_id"),
    col("line_no").cast("int").alias("line_no"),
    trim(col("product_id")).alias("product_id"),
    col("qty").cast("int").alias("qty"),
    col("unit_price").cast("double").alias("unit_price"),
    discount_expr.alias("discount_pct"),
    lower(trim(col("line_type"))).alias("line_type"),
    col("attributes.color").alias("attr_color"),
    col("attributes.size").alias("attr_size"),
    col("attributes.warranty_months").cast("int").alias("attr_warranty_months"),
    col("attributes.volume_ml").cast("int").alias("attr_volume_ml"),
    col("attributes.gift_wrap").cast("boolean").alias("attr_gift_wrap"),
    col("attributes.reason").alias("attr_return_reason"),
    when(trim(col("product_id")).isin(["P101", "P102", "P103", "P105"]), lit(False)).otherwise(lit(True)).alias("is_catalog_matched"),
    col("batch_id"),
    col("ingestion_timestamp")
)

w_item = Window.partitionBy("order_id", "line_no").orderBy(col("ingestion_timestamp").desc_nulls_last())
items_deduped = items_transformed.withColumn("rn", row_number().over(w_item)) \
    .filter(col("rn") == 1) \
    .drop("rn")

items_table = f"{catalog}.{database}.order_items_current"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {items_table} (
    order_id STRING,
    line_no INT,
    product_id STRING,
    qty INT,
    unit_price DOUBLE,
    discount_pct DOUBLE,
    line_type STRING,
    attr_color STRING,
    attr_size STRING,
    attr_warranty_months INT,
    attr_volume_ml INT,
    attr_gift_wrap BOOLEAN,
    attr_return_reason STRING,
    is_catalog_matched BOOLEAN,
    batch_id STRING,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (line_type)
TBLPROPERTIES ('format-version' = '2')
""")

items_deduped.createOrReplaceTempView("src_items")
spark.sql(f"""
MERGE INTO {items_table} t
USING src_items s
ON t.order_id = s.order_id AND t.line_no = s.line_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
order_items_count = spark.table(items_table).count()
print(f">>> order_items_current row count after {batch_id}: {order_items_count}")

# ==============================================================================
# 5. CURATE PAYMENTS (When Batch 2)
# ==============================================================================
payments_table = f"{catalog}.{database}.payments_current"
quarantine_payments_table = f"{catalog}.{database}.payments_quarantine"

if batch_id == "batch_2":
    print("\n>>> 5. Curating payments for batch_2 into Iceberg...")
    bronze_payments = spark.read.parquet(f"s3://{bucket}/bronze/payments/batch_id=batch_2/")
    
    payments_transformed = bronze_payments.select(
        trim(col("payment_id")).alias("payment_id"),
        trim(col("order_id")).alias("order_id"),
        lower(trim(col("method"))).alias("method"),
        col("amount").cast("double").alias("amount"),
        upper(trim(col("currency"))).alias("currency"),
        lower(trim(col("status"))).alias("status"),
        to_timestamp(col("paid_at")).alias("paid_at_utc"),
        trim(col("gateway_ref")).alias("gateway_ref"),
        col("batch_id"),
        col("ingestion_timestamp")
    ).dropDuplicates(["payment_id"])

    # Separate matched vs unmatched (orphan) payments
    known_orders_df = spark.table(orders_table).select("order_id").distinct()
    
    matched_payments = payments_transformed.join(known_orders_df, on="order_id", how="inner")
    unmatched_payments = payments_transformed.join(known_orders_df, on="order_id", how="left_anti") \
        .withColumn("quarantine_reason", lit("UNMATCHED_ORDER_ID")) \
        .withColumn("quarantined_at", current_timestamp())

    print(f">>> Valid payments count: {matched_payments.count()}")
    print(f">>> Quarantined orphan payments count: {unmatched_payments.count()}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {payments_table} (
        payment_id STRING,
        order_id STRING,
        method STRING,
        amount DOUBLE,
        currency STRING,
        status STRING,
        paid_at_utc TIMESTAMP,
        gateway_ref STRING,
        batch_id STRING,
        ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (status)
    TBLPROPERTIES ('format-version' = '2')
    """)

    matched_payments.createOrReplaceTempView("src_payments")
    spark.sql(f"""
    MERGE INTO {payments_table} t
    USING src_payments s
    ON t.payment_id = s.payment_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print(f">>> payments_current row count: {spark.table(payments_table).count()}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {quarantine_payments_table} (
        payment_id STRING,
        order_id STRING,
        method STRING,
        amount DOUBLE,
        currency STRING,
        status STRING,
        paid_at_utc TIMESTAMP,
        gateway_ref STRING,
        batch_id STRING,
        ingestion_timestamp TIMESTAMP,
        quarantine_reason STRING,
        quarantined_at TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
    """)

    unmatched_payments.createOrReplaceTempView("src_quarantine_payments")
    spark.sql(f"""
    MERGE INTO {quarantine_payments_table} t
    USING src_quarantine_payments s
    ON t.payment_id = s.payment_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print(f">>> payments_quarantine row count: {spark.table(quarantine_payments_table).count()}")

# ==============================================================================
# 6. ENRICH FACT_ORDER_LINE (SILVER ENRICHED FACT TABLE)
# ==============================================================================
print("\n>>> 6. Refreshing Silver enriched fact table: novacart.fact_order_line...")
fact_order_line_table = f"{catalog}.{database}.fact_order_line"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fact_order_line_table} (
    order_id STRING,
    line_no INT,
    order_ts_utc TIMESTAMP,
    order_date DATE,
    order_status STRING,
    customer_id STRING,
    shipping_country STRING,
    promo_code STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    qty INT,
    line_type STRING,
    currency STRING,
    unit_price_local DOUBLE,
    discount_pct DOUBLE,
    rate_to_usd DOUBLE,
    line_gross_usd DOUBLE,
    line_net_usd DOUBLE,
    attr_color STRING,
    attr_size STRING,
    attr_return_reason STRING,
    batch_id STRING,
    curated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES ('format-version' = '2')
""")

fact_sql = f"""
SELECT
    i.order_id,
    i.line_no,
    o.order_ts_utc,
    o.order_date,
    o.status AS order_status,
    o.customer_id,
    o.shipping_country,
    o.promo_code,
    i.product_id,
    COALESCE(p.product_name, 'Unknown Product') AS product_name,
    COALESCE(p.category, 'Uncategorized') AS category,
    i.qty,
    i.line_type,
    o.currency,
    i.unit_price AS unit_price_local,
    i.discount_pct,
    COALESCE(fx.rate_to_usd, 1.0) AS rate_to_usd,
    ROUND((i.qty * i.unit_price) * COALESCE(fx.rate_to_usd, 1.0), 4) AS line_gross_usd,
    ROUND(((i.qty * i.unit_price) * (1.0 - (i.discount_pct / 100.0))) * COALESCE(fx.rate_to_usd, 1.0), 4) AS line_net_usd,
    i.attr_color,
    i.attr_size,
    i.attr_return_reason,
    i.batch_id,
    current_timestamp() AS curated_at
FROM {items_table} i
JOIN {orders_table} o ON i.order_id = o.order_id
LEFT JOIN {products_table} p ON i.product_id = p.product_id
LEFT JOIN {fx_table} fx ON o.order_date = fx.rate_date AND o.currency = fx.currency
"""

fact_df = spark.sql(fact_sql)
fact_df.createOrReplaceTempView("src_fact")
spark.sql(f"""
MERGE INTO {fact_order_line_table} t
USING src_fact s
ON t.order_id = s.order_id AND t.line_no = s.line_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f">>> fact_order_line row count: {spark.table(fact_order_line_table).count()}")

# ==============================================================================
# 7. WRITE AUDIT RECORD
# ==============================================================================
end_time = datetime.now(timezone.utc).isoformat()
audit_records = [
    {"batch_id": batch_id, "table": "orders_current", "row_count": spark.table(orders_table).count()},
    {"batch_id": batch_id, "table": "order_items_current", "row_count": spark.table(items_table).count()},
    {"batch_id": batch_id, "table": "fact_order_line", "row_count": spark.table(fact_order_line_table).count()}
]

audit_df = spark.createDataFrame(audit_records) \
    .withColumn("stage", lit("SILVER_CURATION")) \
    .withColumn("start_time", lit(start_time)) \
    .withColumn("end_time", lit(end_time)) \
    .withColumn("status", lit("SUCCESS"))

audit_path = f"s3://{bucket}/control/batch_audit/stage=SILVER/batch_id={batch_id}"
audit_df.write.mode("append").parquet(audit_path)
print(">>> Silver audit log written successfully.")

job.commit()
print(">>> Silver Curation Job Finished Successfully!")
