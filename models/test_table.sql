SELECT
    c.first_name, 
    c.last_name,
    o.customer_id, 
    COUNT(*) AS orders_count
FROM {{ ref('stg_orders') }} AS o
LEFT JOIN {{ ref('stg_customers') }} AS c 
    ON o.customer_id = c.customer_id
GROUP BY 1, 2 ,3

