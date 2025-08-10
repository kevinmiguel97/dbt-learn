WITH import_orders AS (
    SELECT 
        order_id, 
        customer_id, 
        order_date
    FROM {{ ref('stg_orders') }}
),

import_payment AS ( 
    SELECT 
        order_id,
        -- Sum the amount of all successful payments for each order
        SUM(CASE WHEN status = 'success' THEN amount END) AS amount 
    FROM {{ ref('stg_payment') }}
    GROUP BY order_id
),

final AS (
    SELECT 
        o.order_id, 
        o.customer_id,
        o.order_date,
        COALESCE(p.amount, 0) AS amount
    FROM import_orders AS o
    LEFT JOIN import_payment AS p USING (order_id) 
)

SELECT * 
FROM final
