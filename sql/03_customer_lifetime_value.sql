-- NovaCart KPI 3: Top Customer Lifetime Value (CLV)
SELECT 
    customer_id,
    full_name,
    tier,
    country,
    lifetime_orders,
    lifetime_units_purchased,
    lifetime_net_spend_usd,
    first_order_date,
    last_order_date
FROM novacart.gold_customer_lifetime_value
ORDER BY lifetime_net_spend_usd DESC
LIMIT 10;
