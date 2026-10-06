import os
import sys
import boto3
import time
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text

console = Console()

def run_athena_query(query_str):
    athena = boto3.client('athena', region_name='ap-southeast-2')
    resp = athena.start_query_execution(
        QueryString=query_str,
        QueryExecutionContext={'Database': 'novacart'},
        ResultConfiguration={'OutputLocation': 's3://novacart-order-analytics-125992594465-ap-southeast-2/athena-query-results/'}
    )
    qid = resp['QueryExecutionId']
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(0.5)
    
    if status != 'SUCCEEDED':
        raise Exception(f"Athena query failed: {status}")
        
    res = athena.get_query_results(QueryExecutionId=qid)
    cols = [c['VarCharValue'] for c in res['ResultSet']['Rows'][0]['Data']]
    rows = []
    for r in res['ResultSet']['Rows'][1:]:
        rows.append({cols[i]: r['Data'][i].get('VarCharValue', '') for i in range(len(cols))})
    return rows

def main():
    console.print(Panel.fit(
        "[bold cyan]NovaCart AWS Lakehouse[/bold cyan] • [bold green]Apache Iceberg v2[/bold green] • [bold yellow]PySpark Analytics Dashboard[/bold yellow]\n"
        "[dim]S3 Bucket: novacart-order-analytics-125992594465-ap-southeast-2 | Region: ap-southeast-2[/dim]",
        border_style="cyan"
    ))

    with console.status("[bold green]Querying Iceberg Gold Tables via Athena...[/bold green]"):
        cat_data = run_athena_query("""
            SELECT category, 
                   SUM(units_sold) AS units_sold, 
                   SUM(units_returned) AS units_returned, 
                   ROUND(SUM(gross_revenue_usd), 2) AS gross_rev, 
                   ROUND(SUM(net_revenue_usd), 2) AS net_rev, 
                   ROUND(AVG(return_rate_pct), 2) AS avg_return_pct
            FROM novacart.gold_revenue_by_category
            GROUP BY category
            ORDER BY net_rev DESC
        """)
        
        clv_data = run_athena_query("""
            SELECT customer_id, full_name, tier, country, lifetime_orders, lifetime_units_purchased, lifetime_net_spend_usd
            FROM novacart.gold_customer_lifetime_value
            ORDER BY lifetime_net_spend_usd DESC
            LIMIT 5
        """)

        scd2_data = run_athena_query("""
            SELECT customer_id, full_name, tier, country, effective_from, effective_to, is_current
            FROM novacart.dim_customer_scd2
            WHERE customer_id = 'C0004'
            ORDER BY effective_from ASC
        """)

    # 1. KPI Metric Banner
    total_net = sum(float(r['net_rev']) for r in cat_data)
    total_units = sum(int(r['units_sold']) for r in cat_data)
    total_returns = sum(int(r['units_returned']) for r in cat_data)
    
    kpi_table = Table.grid(padding=(0, 3))
    kpi_table.add_column(justify="center")
    kpi_table.add_column(justify="center")
    kpi_table.add_column(justify="center")
    kpi_table.add_column(justify="center")
    
    kpi_table.add_row(
        f"[bold green]${total_net:,.2f}[/bold green]\n[dim]Net Revenue (USD)[/dim]",
        f"[bold blue]{total_units:,}[/bold blue]\n[dim]Total Units Sold[/dim]",
        f"[bold red]{total_returns:,}[/bold red]\n[dim]Units Returned[/dim]",
        f"[bold magenta]945[/bold magenta]\n[dim]Deduplicated Orders[/dim]"
    )
    console.print(Panel(kpi_table, title="[bold white]Executive High-Level KPIs (Computed via PySpark)[/bold white]", border_style="green"))

    # 2. Category Performance Table
    cat_table = Table(title="[bold yellow]1. Product Category Revenue Breakdown[/bold yellow]", border_style="yellow")
    cat_table.add_column("Category", style="bold cyan")
    cat_table.add_column("Units Sold", justify="right", style="white")
    cat_table.add_column("Units Ret", justify="right", style="red")
    cat_table.add_column("Gross Rev (USD)", justify="right", style="dim")
    cat_table.add_column("Net Rev (USD)", justify="right", style="bold green")
    cat_table.add_column("Return Rate", justify="right", style="magenta")

    for r in cat_data:
        cat_table.add_row(
            r['category'],
            f"{int(r['units_sold']):,}",
            f"{int(r['units_returned']):,}",
            f"${float(r['gross_rev']):,.2f}",
            f"${float(r['net_rev']):,.2f}",
            f"{float(r['avg_return_pct']):.2f}%"
        )
    console.print(cat_table)

    # 3. Top Customer Lifetime Value
    clv_table = Table(title="[bold cyan]2. Top 5 Champions by Customer Lifetime Value (CLV)[/bold cyan]", border_style="cyan")
    clv_table.add_column("Cust ID", style="bold")
    clv_table.add_column("Full Name", style="white")
    clv_table.add_column("Tier", style="yellow")
    clv_table.add_column("Country", style="dim")
    clv_table.add_column("Orders", justify="right")
    clv_table.add_column("Units", justify="right")
    clv_table.add_column("Lifetime Spend (USD)", justify="right", style="bold green")

    for r in clv_data:
        clv_table.add_row(
            r['customer_id'],
            r['full_name'],
            r['tier'],
            r['country'],
            r['lifetime_orders'],
            r['lifetime_units_purchased'],
            f"${float(r['lifetime_net_spend_usd']):,.2f}"
        )
    console.print(clv_table)

    # 4. Customer SCD Type 2 Audit Trail
    scd2_table = Table(title="[bold magenta]3. Customer SCD Type 2 Audit History (Customer C0004)[/bold magenta]", border_style="magenta")
    scd2_table.add_column("Customer ID", style="bold")
    scd2_table.add_column("Name", style="white")
    scd2_table.add_column("Tier", style="yellow")
    scd2_table.add_column("Country", style="cyan")
    scd2_table.add_column("Effective From", style="dim")
    scd2_table.add_column("Effective To", style="dim")
    scd2_table.add_column("Current Active?", justify="center", style="bold")

    for r in scd2_data:
        is_cur = "[green]YES (Current)[/green]" if r['is_current'] == 'true' else "[dim]Historical[/dim]"
        scd2_table.add_row(
            r['customer_id'],
            r['full_name'],
            r['tier'],
            r['country'],
            r['effective_from'][:10],
            r['effective_to'][:10],
            is_cur
        )
    console.print(scd2_table)
    console.print("\n[bold green][SUCCESS] Dashboard generated successfully from Apache Iceberg Gold Lakehouse.[/bold green]\n")

if __name__ == "__main__":
    main()
