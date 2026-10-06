-- NovaCart KPI 2: Revenue & Performance by Product Category
SELECT 
    category,
    SUM(units_sold) AS total_units_sold,
    SUM(units_returned) AS total_units_returned,
    ROUND(SUM(gross_revenue_usd), 2) AS gross_revenue_usd,
    ROUND(SUM(net_revenue_usd), 2) AS net_revenue_usd,
    ROUND(AVG(return_rate_pct), 2) AS avg_return_rate_pct
FROM novacart.gold_revenue_by_category
GROUP BY category
ORDER BY net_revenue_usd DESC;
