SELECT 
    id AS payment_id, 
    orderid AS order_id, 
    paymentmethod AS payment_method, 
    status AS payment_status, 
    -- Amount is stored in cents convert to dls 
    amount / 100 AS payment_amount, 
    created AS created_at, 
FROM {{ source('stripe', 'payment') }}
