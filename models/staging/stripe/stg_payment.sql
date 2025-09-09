SELECT 
    id AS payment_id, 
    orderid AS order_id, 
    paymentmethod AS payment_method, 
    status, 
    -- Amount is stored in cents convert to dls 
    {{ cents_to_dollars("amount") }} AS amount, 
    created AS created_at, 
FROM {{ source('stripe', 'payment') }}
