SELECT
    customer_id, 
    order_date,
    -- Using a dbt_utils macro to generate a unique key 
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'order_date']) }} AS primary_key,
    COUNT(*) AS day_orders
FROM {{ ref('stg_orders') }}
GROUP BY 1, 2 
ORDER BY 2 DESC, 3 ASC