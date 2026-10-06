import boto3
import time

athena = boto3.client('athena', region_name='ap-southeast-2')
bucket = 'novacart-order-analytics-125992594465-ap-southeast-2'
s3_output = f's3://{bucket}/athena-query-results/'

def query(sql, title):
    print(f"\n=======================================================")
    print(f" {title}")
    print(f"=======================================================")
    print(f"SQL: {sql.strip()}")
    res = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': 'novacart'},
        ResultConfiguration={'OutputLocation': s3_output}
    )
    qid = res['QueryExecutionId']
    while True:
        status_res = athena.get_query_execution(QueryExecutionId=qid)
        state = status_res['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if state != 'SUCCEEDED':
        reason = status_res['QueryExecution']['Status'].get('StateChangeReason')
        raise Exception(f"Query failed ({state}): {reason}")
    
    results = athena.get_query_results(QueryExecutionId=qid)
    rows = results['ResultSet']['Rows']
    print(f"Rows returned: {len(rows)-1}\n")
    for r in rows:
        print("  ", [d.get('VarCharValue', 'NULL') for d in r['Data']])
    return len(rows) - 1

def run_tests():
    # 1. Total KPI row counts
    query("SELECT COUNT(*) AS total_days, ROUND(SUM(net_revenue_usd), 2) AS total_lakehouse_net_revenue_usd FROM novacart.gold_daily_revenue;", "TEST G.1: Overall Lakehouse Net Revenue & Active Days")
    
    # 2. Daily revenue top days
    query("SELECT order_date, total_orders, gross_sales_usd, returns_usd, net_revenue_usd FROM novacart.gold_daily_revenue ORDER BY order_date DESC LIMIT 5;", "TEST G.2: Daily Revenue Timeline (Recent 5 Days)")

    # 3. Category revenue performance
    query("SELECT category, SUM(units_sold) AS total_units_sold, SUM(units_returned) AS total_units_returned, ROUND(SUM(net_revenue_usd), 2) AS total_net_revenue_usd, ROUND(AVG(return_rate_pct), 2) AS avg_return_rate_pct FROM novacart.gold_revenue_by_category GROUP BY category ORDER BY total_net_revenue_usd DESC;", "TEST G.3: Revenue & Return Performance by Product Category")

    # 4. Top 5 highest lifetime value customers
    query("SELECT customer_id, full_name, tier, country, lifetime_orders, lifetime_units_purchased, lifetime_net_spend_usd, first_order_date, last_order_date FROM novacart.gold_customer_lifetime_value ORDER BY lifetime_net_spend_usd DESC LIMIT 5;", "TEST G.4: Top 5 Highest Lifetime Value Customers")

    # 5. Product return reasons analysis
    query("SELECT product_name, category, total_sales_units, total_returns_units, return_rate_pct, damaged_returns, wrong_size_returns, not_as_described_returns, changed_mind_returns FROM novacart.gold_product_return_rates WHERE total_returns_units > 0 ORDER BY total_returns_units DESC LIMIT 5;", "TEST G.5: Top Returned Products & Root Cause Breakdown")

if __name__ == '__main__':
    run_tests()
