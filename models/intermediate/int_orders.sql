WITH dedup_orders AS (

    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY order_time DESC
            ) AS rn
        FROM {{ ref('base_web_orders') }}
    )
    WHERE rn = 1

),

returns_cleaned AS (

    SELECT
        order_id,
        MAX(
            CASE
                WHEN is_refunded = TRUE THEN TRUE
                ELSE FALSE
            END
        ) AS is_refunded_bool,
        MAX(is_refunded) AS is_refunded
    FROM {{ ref('base_google_drive_returns') }}
    GROUP BY order_id

),

final AS (

    SELECT
        o.order_id,
        o.order_time,
        r.is_refunded,
        COALESCE(r.is_refunded_bool, FALSE) AS is_refunded_bool
    FROM dedup_orders o
    LEFT JOIN returns_cleaned r
        ON o.order_id = r.order_id

)

SELECT * FROM final