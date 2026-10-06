-- NovaCart KPI 4: Product Return Rates & Root Cause Breakdown
SELECT 
    product_name,
    category,
    total_sales_units,
    total_returns_units,
    return_rate_pct,
    damaged_returns,
    wrong_size_returns,
    not_as_described_returns,
    changed_mind_returns
FROM novacart.gold_product_return_rates
WHERE total_returns_units > 0
ORDER BY return_rate_pct DESC
LIMIT 10;
