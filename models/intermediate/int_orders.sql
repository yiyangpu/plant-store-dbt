WITH dedup_orders AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY order_time DESC, _fivetran_synced DESC, _fivetran_id DESC
            ) AS rn
        FROM {{ ref('base_web_orders') }}
        WHERE order_id IS NOT NULL
    )
    WHERE rn = 1

),

item_views_by_session AS (

    SELECT
        session_id,
        COUNT(DISTINCT CASE WHEN add_to_cart_quantity > 0 THEN item_name END) AS distinct_items_ordered,
        SUM(add_to_cart_quantity) AS total_add_to_cart_quantity,
        SUM(remove_from_cart_quantity) AS total_remove_from_cart_quantity,
        SUM(price_per_unit * add_to_cart_quantity) AS gross_added_amount,
        SUM(price_per_unit * remove_from_cart_quantity) AS removed_amount,
        SUM(price_per_unit * add_to_cart_quantity)
            - SUM(price_per_unit * remove_from_cart_quantity) AS net_item_amount
    FROM {{ ref('base_web_item_views') }}
    GROUP BY session_id

),

returns_cleaned AS (

    SELECT
        order_id,
        MAX(returned_date) AS returned_date,
        MAX(
            CASE
                WHEN LOWER(CAST(is_refunded AS STRING)) IN ('yes', 'true', 'y', '1') THEN TRUE
                ELSE FALSE
            END
        ) AS is_refunded
    FROM {{ ref('base_google_drive_returns') }}
    WHERE order_id IS NOT NULL
    GROUP BY order_id

),

final AS (

    SELECT
        o.order_id,
        o.session_id,
        o.client_name,
        o.order_time,
        o.payment_method,
        o.shipping_address,
        o.state,

        COALESCE(i.distinct_items_ordered, 0) AS distinct_items_ordered,
        COALESCE(i.total_add_to_cart_quantity, 0) AS total_add_to_cart_quantity,
        COALESCE(i.total_remove_from_cart_quantity, 0) AS total_remove_from_cart_quantity,
        COALESCE(i.gross_added_amount, 0) AS gross_added_amount,
        COALESCE(i.removed_amount, 0) AS removed_amount,
        COALESCE(i.net_item_amount, 0) AS net_item_amount,

        COALESCE(o.shipping_cost, 0) AS shipping_cost,
        COALESCE(o.tax_rate, 0) AS tax_rate,
        COALESCE(i.net_item_amount, 0) * COALESCE(o.tax_rate, 0) AS tax_amount,
        COALESCE(i.net_item_amount, 0)
            + COALESCE(o.shipping_cost, 0)
            + (COALESCE(i.net_item_amount, 0) * COALESCE(o.tax_rate, 0)) AS gross_order_amount,

        COALESCE(r.is_refunded, FALSE) AS is_refunded,
        r.returned_date,

        CASE
            WHEN COALESCE(r.is_refunded, FALSE) = TRUE THEN 0
            ELSE
                COALESCE(i.net_item_amount, 0)
                + COALESCE(o.shipping_cost, 0)
                + (COALESCE(i.net_item_amount, 0) * COALESCE(o.tax_rate, 0))
        END AS net_revenue

    FROM dedup_orders o
    LEFT JOIN item_views_by_session i
        ON o.session_id = i.session_id
    LEFT JOIN returns_cleaned r
        ON o.order_id = r.order_id

)

SELECT * FROM final