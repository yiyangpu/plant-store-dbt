WITH orders AS (
    SELECT *
    FROM {{ ref('base_web_orders') }}
),

returns AS (
    SELECT *
    FROM {{ ref('base_google_drive_returns') }}
),

dedup_orders AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY order_time DESC
               ) AS rn
        FROM orders
    )
    WHERE rn = 1
),

final AS (
    SELECT
        o.order_id,
        o.order_time,
        r.is_refunded,
        CASE
            WHEN r.is_refunded = TRUE THEN TRUE
            ELSE FALSE
        END AS is_refunded_bool
    FROM dedup_orders o
    LEFT JOIN returns r
        ON o.order_id = r.order_id
)

SELECT * FROM final