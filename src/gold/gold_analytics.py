import sys
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lit, when, sum as spark_sum, countDistinct, count, round as spark_round,
    min as spark_min, max as spark_max, abs as spark_abs, coalesce, current_timestamp
)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])
bucket = args['BUCKET_NAME']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

start_time = datetime.now(timezone.utc).isoformat()
print(f">>> [Gold Analytics] Starting Gold KPI aggregations at {start_time}")

catalog = "glue_catalog"
database = "novacart"

# Source Silver tables
fact_table = f"{catalog}.{database}.fact_order_line"
dim_cust_table = f"{catalog}.{database}.dim_customer_scd2"

fact_df = spark.table(fact_table)
cust_df = spark.table(dim_cust_table).filter(col("is_current") == True)

# ==============================================================================
# 1. GOLD: DAILY REVENUE (USD)
# ==============================================================================
print("\n>>> 1. Building Gold Daily Revenue KPI table...")
daily_rev_table = f"{catalog}.{database}.gold_daily_revenue"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {daily_rev_table} (
    order_date DATE,
    total_orders INT,
    gross_sales_usd DOUBLE,
    returns_usd DOUBLE,
    net_revenue_usd DOUBLE,
    curated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES ('format-version' = '2')
""")

daily_rev_df = fact_df.groupBy("order_date").agg(
    countDistinct("order_id").alias("total_orders"),
    spark_round(spark_sum(when(col("line_type") == "sale", col("line_gross_usd")).otherwise(0.0)), 2).alias("gross_sales_usd"),
    spark_round(spark_sum(when(col("line_type") == "return", spark_abs(col("line_net_usd"))).otherwise(0.0)), 2).alias("returns_usd"),
    spark_round(spark_sum(col("line_net_usd")), 2).alias("net_revenue_usd")
).withColumn("curated_at", current_timestamp())

daily_rev_df.createOrReplaceTempView("src_daily_rev")
spark.sql(f"""
MERGE INTO {daily_rev_table} t
USING src_daily_rev s
ON t.order_date = s.order_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f">>> gold_daily_revenue row count: {spark.table(daily_rev_table).count()}")

# ==============================================================================
# 2. GOLD: REVENUE BY CATEGORY & PRODUCT
# ==============================================================================
print("\n>>> 2. Building Gold Revenue by Category table...")
cat_rev_table = f"{catalog}.{database}.gold_revenue_by_category"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cat_rev_table} (
    category STRING,
    product_id STRING,
    product_name STRING,
    units_sold INT,
    units_returned INT,
    gross_revenue_usd DOUBLE,
    net_revenue_usd DOUBLE,
    return_rate_pct DOUBLE,
    curated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES ('format-version' = '2')
""")

cat_rev_df = fact_df.groupBy("category", "product_id", "product_name").agg(
    spark_sum(when(col("line_type") == "sale", col("qty")).otherwise(0)).alias("units_sold"),
    spark_sum(when(col("line_type") == "return", spark_abs(col("qty"))).otherwise(0)).alias("units_returned"),
    spark_round(spark_sum(when(col("line_type") == "sale", col("line_gross_usd")).otherwise(0.0)), 2).alias("gross_revenue_usd"),
    spark_round(spark_sum(col("line_net_usd")), 2).alias("net_revenue_usd")
).withColumn(
    "return_rate_pct",
    spark_round((col("units_returned") / when((col("units_sold") + col("units_returned")) > 0, col("units_sold") + col("units_returned")).otherwise(1)) * 100.0, 2)
).withColumn("curated_at", current_timestamp())

cat_rev_df.createOrReplaceTempView("src_cat_rev")
spark.sql(f"""
MERGE INTO {cat_rev_table} t
USING src_cat_rev s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f">>> gold_revenue_by_category row count: {spark.table(cat_rev_table).count()}")

# ==============================================================================
# 3. GOLD: CUSTOMER LIFETIME VALUE (CLV)
# ==============================================================================
print("\n>>> 3. Building Gold Customer Lifetime Value table...")
clv_table = f"{catalog}.{database}.gold_customer_lifetime_value"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {clv_table} (
    customer_id STRING,
    full_name STRING,
    tier STRING,
    country STRING,
    lifetime_orders INT,
    lifetime_units_purchased INT,
    lifetime_net_spend_usd DOUBLE,
    first_order_date DATE,
    last_order_date DATE,
    curated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (tier)
TBLPROPERTIES ('format-version' = '2')
""")

cust_orders_agg = fact_df.groupBy("customer_id").agg(
    countDistinct("order_id").alias("lifetime_orders"),
    spark_sum(when(col("line_type") == "sale", col("qty")).otherwise(0)).alias("lifetime_units_purchased"),
    spark_round(spark_sum(col("line_net_usd")), 2).alias("lifetime_net_spend_usd"),
    spark_min("order_date").alias("first_order_date"),
    spark_max("order_date").alias("last_order_date")
)

clv_df = cust_orders_agg.join(cust_df, on="customer_id", how="left") \
    .select(
        col("customer_id"),
        coalesce(col("full_name"), lit("Unknown Customer")).alias("full_name"),
        coalesce(col("tier"), lit("Standard")).alias("tier"),
        coalesce(col("country"), lit("UNKNOWN")).alias("country"),
        col("lifetime_orders"),
        col("lifetime_units_purchased"),
        col("lifetime_net_spend_usd"),
        col("first_order_date"),
        col("last_order_date"),
        current_timestamp().alias("curated_at")
    )

clv_df.createOrReplaceTempView("src_clv")
spark.sql(f"""
MERGE INTO {clv_table} t
USING src_clv s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f">>> gold_customer_lifetime_value row count: {spark.table(clv_table).count()}")

# ==============================================================================
# 4. GOLD: PRODUCT RETURN RATES & REASON ANALYSIS
# ==============================================================================
print("\n>>> 4. Building Gold Product Return Rates & Reasons table...")
return_rate_table = f"{catalog}.{database}.gold_product_return_rates"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {return_rate_table} (
    product_id STRING,
    product_name STRING,
    category STRING,
    total_sales_units INT,
    total_returns_units INT,
    return_rate_pct DOUBLE,
    damaged_returns INT,
    wrong_size_returns INT,
    not_as_described_returns INT,
    changed_mind_returns INT,
    curated_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2')
""")

return_df = fact_df.groupBy("product_id", "product_name", "category").agg(
    spark_sum(when(col("line_type") == "sale", col("qty")).otherwise(0)).alias("total_sales_units"),
    spark_sum(when(col("line_type") == "return", spark_abs(col("qty"))).otherwise(0)).alias("total_returns_units"),
    spark_sum(when((col("line_type") == "return") & (col("attr_return_reason") == "damaged"), spark_abs(col("qty"))).otherwise(0)).alias("damaged_returns"),
    spark_sum(when((col("line_type") == "return") & (col("attr_return_reason") == "wrong_size"), spark_abs(col("qty"))).otherwise(0)).alias("wrong_size_returns"),
    spark_sum(when((col("line_type") == "return") & (col("attr_return_reason") == "not_as_described"), spark_abs(col("qty"))).otherwise(0)).alias("not_as_described_returns"),
    spark_sum(when((col("line_type") == "return") & (col("attr_return_reason") == "changed_mind"), spark_abs(col("qty"))).otherwise(0)).alias("changed_mind_returns")
).withColumn(
    "return_rate_pct",
    spark_round((col("total_returns_units") / when((col("total_sales_units") + col("total_returns_units")) > 0, col("total_sales_units") + col("total_returns_units")).otherwise(1)) * 100.0, 2)
).withColumn("curated_at", current_timestamp())

return_df.createOrReplaceTempView("src_returns")
spark.sql(f"""
MERGE INTO {return_rate_table} t
USING src_returns s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f">>> gold_product_return_rates row count: {spark.table(return_rate_table).count()}")

# ==============================================================================
# AUDIT LOG
# ==============================================================================
end_time = datetime.now(timezone.utc).isoformat()
audit_records = [
    {"table": "gold_daily_revenue", "row_count": spark.table(daily_rev_table).count()},
    {"table": "gold_revenue_by_category", "row_count": spark.table(cat_rev_table).count()},
    {"table": "gold_customer_lifetime_value", "row_count": spark.table(clv_table).count()},
    {"table": "gold_product_return_rates", "row_count": spark.table(return_rate_table).count()}
]

audit_df = spark.createDataFrame(audit_records) \
    .withColumn("stage", lit("GOLD_AGGREGATION")) \
    .withColumn("start_time", lit(start_time)) \
    .withColumn("end_time", lit(end_time)) \
    .withColumn("status", lit("SUCCESS"))

audit_path = f"s3://{bucket}/control/batch_audit/stage=GOLD"
audit_df.write.mode("append").parquet(audit_path)

job.commit()
print(">>> Gold Analytics Processing Completed Successfully!")
