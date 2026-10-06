import pytest
import boto3
import time

REGION = 'ap-southeast-2'
BUCKET = f'novacart-order-analytics-125992594465-{REGION}'
DATABASE = 'novacart'

s3 = boto3.client('s3', region_name=REGION)
glue = boto3.client('glue', region_name=REGION)
athena = boto3.client('athena', region_name=REGION)

def query_athena(sql):
    res = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': f's3://{BUCKET}/athena-query-results/'}
    )
    qid = res['QueryExecutionId']
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    assert status == 'SUCCEEDED'
    results = athena.get_query_results(QueryExecutionId=qid)
    return int(results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])

def test_s3_bucket_and_encryption():
    enc = s3.get_bucket_encryption(Bucket=BUCKET)
    assert enc['ServerSideEncryptionConfiguration']['Rules'][0]['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'] == 'AES256'

def test_s3_public_access_block():
    pab = s3.get_public_access_block(Bucket=BUCKET)['PublicAccessBlockConfiguration']
    assert pab['BlockPublicAcls'] is True
    assert pab['IgnorePublicAcls'] is True
    assert pab['BlockPublicPolicy'] is True
    assert pab['RestrictPublicBuckets'] is True

def test_glue_database():
    db = glue.get_database(Name=DATABASE)['Database']
    assert db['Name'] == DATABASE

def test_bronze_table_counts():
    # Batch 1 counts
    assert query_athena("SELECT COUNT(*) FROM novacart.bronze_orders WHERE batch_id = 'batch_1';") == 1418
    assert query_athena("SELECT COUNT(*) FROM novacart.bronze_order_items WHERE batch_id = 'batch_1';") == 1006
    # Batch 2 counts
    assert query_athena("SELECT COUNT(*) FROM novacart.bronze_orders WHERE batch_id = 'batch_2';") == 1545
    assert query_athena("SELECT COUNT(*) FROM novacart.bronze_order_items WHERE batch_id = 'batch_2';") == 1007
    # Total Bronze tables
    assert query_athena("SELECT COUNT(*) FROM novacart.bronze_customers;") == 194
    assert query_athena("SELECT COUNT(*) FROM novacart.bronze_fx_rates;") == 84
    assert query_athena("SELECT COUNT(*) FROM novacart.bronze_products;") == 40
    assert query_athena("SELECT COUNT(*) FROM novacart.bronze_payments;") == 1164

def test_silver_orders_current():
    # 495 from Batch 1 + 450 new from Batch 2 = 945 unique orders
    assert query_athena("SELECT COUNT(*) FROM novacart.orders_current;") == 945

def test_silver_order_items_current():
    # Exactly 1,951 unique (order_id, line_no) composite keys after deduplication across batches
    assert query_athena("SELECT COUNT(*) FROM novacart.order_items_current;") == 1951

def test_silver_payments():
    # 1,159 valid payments + 5 quarantined orphan payments
    assert query_athena("SELECT COUNT(*) FROM novacart.payments_current;") == 1159
    assert query_athena("SELECT COUNT(*) FROM novacart.payments_quarantine;") == 5

def test_silver_scd2_customers():
    # Exactly 130 active customers (is_current = true) and 60 historical records
    assert query_athena("SELECT COUNT(*) FROM novacart.dim_customer_scd2 WHERE is_current = true;") == 130
    assert query_athena("SELECT COUNT(*) FROM novacart.dim_customer_scd2 WHERE is_current = false;") == 60

def test_gold_kpi_tables():
    # 30 active days in September 2026
    assert query_athena("SELECT COUNT(*) FROM novacart.gold_daily_revenue;") == 30
    assert query_athena("SELECT COUNT(*) FROM novacart.gold_revenue_by_category;") > 0
    assert query_athena("SELECT COUNT(*) FROM novacart.gold_customer_lifetime_value;") > 0
    # 40 catalog products + 4 uncataloged products = 44 products
    assert query_athena("SELECT COUNT(*) FROM novacart.gold_product_return_rates;") == 44
