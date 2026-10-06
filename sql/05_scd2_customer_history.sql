-- NovaCart Dimension Query: Customer SCD Type 2 Audit Trail
SELECT 
    customer_id,
    full_name,
    email,
    tier,
    country,
    effective_from,
    effective_to,
    is_current
FROM novacart.dim_customer_scd2
WHERE customer_id = 'C0004'
ORDER BY effective_from ASC;
