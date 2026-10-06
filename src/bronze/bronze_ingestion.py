import sys
import boto3
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, current_timestamp

# Resolve arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'BATCH_ID'])
bucket = args['BUCKET_NAME']
batch_id = args['BATCH_ID']
region = "ap-southeast-2"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glue_client = boto3.client('glue', region_name=region)

start_time = datetime.now(timezone.utc).isoformat()
print(f">>> [Bronze Ingestion] Starting ingestion for {batch_id} at {start_time}")

audit_records = []

def register_glue_catalog_table(name, schema, location):
    columns = []
    for field in schema.fields:
        st = field.dataType.simpleString().lower()
        if "int" in st:
            gt = "int"
        elif "double" in st or "float" in st:
            gt = "double"
        elif "timestamp" in st:
            gt = "timestamp"
        elif "date" in st:
            gt = "date"
        elif "boolean" in st:
            gt = "boolean"
        else:
            gt = "string"
        columns.append({'Name': field.name, 'Type': gt})

    table_input = {
        'Name': f'bronze_{name}',
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'parquet'
        },
        'StorageDescriptor': {
            'Columns': columns,
            'Location': location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'Compressed': False,
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            }
        }
    }
    try:
        glue_client.create_table(DatabaseName='novacart', TableInput=table_input)
        print(f">>> Registered Glue Data Catalog table: novacart.bronze_{name}")
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_table(DatabaseName='novacart', TableInput=table_input)
        print(f">>> Updated Glue Data Catalog table: novacart.bronze_{name}")

def ingest_dataset(name, landing_rel_path, format_type, is_multiline=False):
    landing_path = f"s3://{bucket}/landing/{batch_id}/{landing_rel_path}"
    bronze_base_path = f"s3://{bucket}/bronze/{name}/"
    bronze_partition_path = f"s3://{bucket}/bronze/{name}/batch_id={batch_id}/"
    
    print(f"\n>>> Ingesting {name} from {landing_path}...")
    try:
        if format_type == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "false").csv(landing_path)
        elif format_type == "json":
            if is_multiline:
                df = spark.read.option("multiLine", "true").json(landing_path)
            else:
                df = spark.read.json(landing_path)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
        
        input_count = df.count()
        print(f">>> Read {input_count} raw records from {landing_rel_path}")
        
        # Attach mandatory Bronze lineage metadata
        bronze_df = df \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("source_file", lit(landing_rel_path)) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        # Write to Bronze S3 storage as Parquet partitioned by batch_id
        print(f">>> Writing Bronze Parquet to {bronze_partition_path}...")
        bronze_df.write.mode("overwrite").parquet(bronze_partition_path)
        
        output_count = spark.read.parquet(bronze_partition_path).count()
        print(f">>> Verified {output_count} records written to {bronze_partition_path}")
        assert input_count == output_count, f"Count mismatch for {name}: input={input_count}, output={output_count}"
        
        # Register in Glue Data Catalog
        register_glue_catalog_table(name, bronze_df.schema, bronze_base_path)
        
        audit_records.append({
            "batch_id": batch_id,
            "dataset": name,
            "source_file": landing_rel_path,
            "input_count": input_count,
            "output_count": output_count,
            "quarantined_count": 0,
            "status": "SUCCESS"
        })
        return input_count
    except Exception as e:
        print(f"!!! Error ingesting {name}: {str(e)}")
        audit_records.append({
            "batch_id": batch_id,
            "dataset": name,
            "source_file": landing_rel_path,
            "input_count": 0,
            "output_count": 0,
            "quarantined_count": 0,
            "status": "FAILED",
            "error": str(e)
        })
        raise e

# Ingest Datasets
if batch_id == "batch_1":
    ingest_dataset("orders", "orders_batch_1.csv", "csv")
    ingest_dataset("order_items", "order_items_batch_1_json.txt", "json", is_multiline=False)
    ingest_dataset("customers", "customers_changes.csv", "csv")
    ingest_dataset("fx_rates", "fx_rates.csv", "csv")
    ingest_dataset("products", "products.csv", "csv")

elif batch_id == "batch_2":
    ingest_dataset("orders", "orders_batch_2.csv", "csv")
    ingest_dataset("order_items", "order_items_batch_2_jsonl.txt", "json", is_multiline=False)
    ingest_dataset("payments", "payments_json.txt", "json", is_multiline=True)

end_time = datetime.now(timezone.utc).isoformat()

# Write audit log to control/batch_audit/
print(f"\n>>> Writing audit log for {batch_id}...")
audit_df = spark.createDataFrame(audit_records) \
    .withColumn("stage", lit("BRONZE_INGESTION")) \
    .withColumn("start_time", lit(start_time)) \
    .withColumn("end_time", lit(end_time))

audit_path = f"s3://{bucket}/control/batch_audit/stage=BRONZE/batch_id={batch_id}"
audit_df.write.mode("append").parquet(audit_path)
print(">>> Audit log written successfully.")

job.commit()
print(">>> Bronze Ingestion Job Finished Successfully!")
