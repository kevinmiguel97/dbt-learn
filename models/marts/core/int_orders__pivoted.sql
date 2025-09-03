{%- set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] -%}

with import_payments AS (
    SELECT *
    from {{ ref('stg_payment') }}
)
, pivoted AS (
    SELECT 
        order_id,

        {% for m in payment_methods %}
 
        SUM(
            CASE
                WHEN payment_method = '{{ m }}' THEN amount 
                ELSE 0
            END
        ) AS {{ m }}_amount

        {%- if not loop.last -%}
        ,
        {%- endif -%}
            
        {% endfor %}

    FROM import_payments
    GROUP BY 1
    ORDER BY 1
)

SELECT * from pivoted