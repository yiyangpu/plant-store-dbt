WITH final AS (
    SELECT
        order_id,
        session_id,
        client_name,
        order_time,
        payment_method,
        state,
        net_item_amount,
        shipping_cost,
        tax_amount,
        gross_order_amount,
        is_refunded,
        returned_date,
        net_revenue
    FROM {{ ref('int_orders') }}
)
SELECT * FROM final