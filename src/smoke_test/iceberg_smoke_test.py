import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(">>> Starting NovaCart Iceberg Smoke Test Job...")

# Database and Table identifiers
catalog = "glue_catalog"
database = "novacart"
table_name = "environment_smoke_test"
full_table_name = f"{catalog}.{database}.{table_name}"

print(f">>> Target table: {full_table_name}")

# 1. Drop existing smoke-test table if present to ensure clean state
print(">>> Step 1: Dropping table if already exists for clean deterministic validation...")
spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

# 2. Create minimal Iceberg smoke-test table
print(">>> Step 2: Creating test Iceberg table...")
create_table_sql = f"""
CREATE TABLE {full_table_name} (
    id INT,
    name STRING,
    created_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.object-storage.enabled' = 'true'
)
"""
spark.sql(create_table_sql)
print(">>> Table created successfully.")

# 3. Insert 3 deterministic test records
print(">>> Step 3: Inserting 3 deterministic sample rows...")
insert_sql = f"""
INSERT INTO {full_table_name} VALUES
    (1, 'NovaCart Alpha', CAST('2026-10-06 10:00:00' AS TIMESTAMP)),
    (2, 'NovaCart Beta', CAST('2026-10-06 11:00:00' AS TIMESTAMP)),
    (3, 'NovaCart Gamma', CAST('2026-10-06 12:00:00' AS TIMESTAMP))
"""
spark.sql(insert_sql)
print(">>> 3 rows inserted successfully.")

# 4. Read records back
print(">>> Step 4: Reading records back from Iceberg table...")
df = spark.sql(f"SELECT * FROM {full_table_name} ORDER BY id")
df.show()

# 5. Count verification
row_count = spark.sql(f"SELECT COUNT(*) AS total FROM {full_table_name}").collect()[0]['total']
print(f">>> Step 5: SELECT COUNT(*) verification result: {row_count}")

assert row_count == 3, f"Expected 3 rows, but got {row_count}"
print(">>> Validation assertion passed: exact 3 rows confirmed in Apache Iceberg table.")

job.commit()
print(">>> NovaCart Iceberg Smoke Test Job completed successfully!")
