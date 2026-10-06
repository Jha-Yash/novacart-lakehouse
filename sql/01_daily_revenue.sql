-- NovaCart KPI 1: Daily Revenue Trend (September 2026)
SELECT 
    order_date,
    total_orders,
    gross_sales_usd,
    returns_usd,
    net_revenue_usd,
    ROUND(returns_usd / NULLIF(gross_sales_usd, 0) * 100.0, 2) AS return_ratio_pct
FROM novacart.gold_daily_revenue
ORDER BY order_date ASC;
