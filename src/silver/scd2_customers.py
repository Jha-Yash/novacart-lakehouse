import sys
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lit, when, upper, lower, trim, coalesce, to_timestamp,
    lead, sha2, concat_ws, current_timestamp
)
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])
bucket = args['BUCKET_NAME']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

start_time = datetime.now(timezone.utc).isoformat()
print(f">>> [SCD2 Customers] Starting Customer SCD Type 2 processing at {start_time}")

catalog = "glue_catalog"
database = "novacart"
target_table = f"{catalog}.{database}.dim_customer_scd2"

# Read Bronze customers
bronze_customers = spark.read.parquet(f"s3://{bucket}/bronze/customers/")
print(f">>> Read {bronze_customers.count()} raw customer change records from Bronze")

# Clean and standardize fields
clean_ts_expr = when(col("updated_at").isNull() | (trim(col("updated_at")) == ""), to_timestamp(lit("1970-01-01 00:00:00"))) \
    .when(col("updated_at").like("%T%Z"), to_timestamp(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .otherwise(to_timestamp(col("updated_at")))

customers_cleaned = bronze_customers.select(
    trim(col("customer_id")).alias("customer_id"),
    trim(col("full_name")).alias("full_name"),
    lower(trim(col("email"))).alias("email"),
    trim(col("tier")).alias("tier"),
    upper(trim(col("country"))).alias("country"),
    clean_ts_expr.alias("effective_from")
).dropDuplicates(["customer_id", "effective_from", "tier", "country", "email"])

# Compute SCD Type 2 Window
w = Window.partitionBy("customer_id").orderBy(col("effective_from").asc())

customers_scd2 = customers_cleaned \
    .withColumn("effective_to", lead(col("effective_from")).over(w)) \
    .withColumn("is_current", when(col("effective_to").isNull(), lit(True)).otherwise(lit(False))) \
    .withColumn("customer_sk", sha2(concat_ws("||", col("customer_id"), col("effective_from")), 256)) \
    .withColumn("created_at_utc", current_timestamp())

# Create Iceberg SCD2 table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    customer_sk STRING,
    customer_id STRING,
    full_name STRING,
    email STRING,
    tier STRING,
    country STRING,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current BOOLEAN,
    created_at_utc TIMESTAMP
)
USING iceberg
PARTITIONED BY (is_current)
TBLPROPERTIES ('format-version' = '2')
""")

customers_scd2.createOrReplaceTempView("src_scd2")
spark.sql(f"""
MERGE INTO {target_table} t
USING src_scd2 s
ON t.customer_sk = s.customer_sk
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

total_dim_count = spark.table(target_table).count()
current_count = spark.table(target_table).filter(col("is_current") == True).count()
historical_count = spark.table(target_table).filter(col("is_current") == False).count()

print(f">>> SCD2 Processing Complete!")
print(f">>> Total dimension records: {total_dim_count}")
print(f">>> Current active versions (is_current=True): {current_count}")
print(f">>> Historical inactive versions (is_current=False): {historical_count}")

# Demonstrate customer C0004 multi-version history
print("\n>>> Multi-version demonstration for Customer C0004:")
spark.sql(f"SELECT customer_id, full_name, email, country, effective_from, effective_to, is_current FROM {target_table} WHERE customer_id = 'C0004' ORDER BY effective_from").show(truncate=False)

job.commit()
print(">>> SCD2 Customers Job Completed Successfully!")
