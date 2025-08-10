WITH import_orders AS (
    SELECT 
        order_id, 
        customer_id
    FROM {{ ref('stg_orders') }}
)

, import_payment AS ( 
    SELECT 
        order_id,
        amount, 
        status 
    FROM {{ ref('stg_payment') }}
)

, final AS (
    SELECT 
        o.order_id, 
        o.customer_id, 
        -- Sum the amount of all successful payments for each order
        SUM (CASE WHEN p.status = 'success' THEN p.amount END) AS amount
    FROM import_orders AS o
    LEFT JOIN import_payment AS p USING (order_id) 
    GROUP BY o.order_id, o.customer_id
)

SELECT * 
FROM final
