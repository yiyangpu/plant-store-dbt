WITH dedup_sessions AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY session_id
                ORDER BY session_time
            ) AS rn
        FROM {{ ref('base_web_sessions') }}
    )
    WHERE rn = 1
),

page_views AS (
    SELECT
        session_id,
        COUNT(*) AS total_page_views,
        TRUE AS has_page_view
    FROM {{ ref('base_web_page_views') }}
    GROUP BY session_id
),

item_views AS (
    SELECT
        session_id,
        COUNT(*) AS total_items_viewed,
        TRUE AS has_item_view,
        MAX(CASE WHEN add_to_cart_quantity > 0 THEN TRUE ELSE FALSE END) AS has_add_to_cart,
        SUM(add_to_cart_quantity) AS total_add_to_cart_quantity,
        SUM(remove_from_cart_quantity) AS total_remove_from_cart_quantity
    FROM {{ ref('base_web_item_views') }}
    GROUP BY session_id
),

dedup_orders AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY order_time
            ) AS rn
        FROM {{ ref('base_web_orders') }}
    )
    WHERE rn = 1
),

orders AS (
    SELECT
        session_id,
        COUNT(*) AS order_count,
        TRUE AS has_order
    FROM dedup_orders
    GROUP BY session_id
),

final AS (
    SELECT
        s.session_id,
        s.client_id,
        s.session_time,
        COALESCE(p.has_page_view, FALSE) AS has_page_view,
        COALESCE(i.has_item_view, FALSE) AS has_item_view,
        COALESCE(i.has_add_to_cart, FALSE) AS has_add_to_cart,
        COALESCE(o.has_order, FALSE) AS has_order,
        COALESCE(p.total_page_views, 0) AS total_page_views,
        COALESCE(i.total_items_viewed, 0) AS total_items_viewed,
        COALESCE(i.total_add_to_cart_quantity, 0) AS total_add_to_cart_quantity,
        COALESCE(i.total_remove_from_cart_quantity, 0) AS total_remove_from_cart_quantity,
        COALESCE(o.order_count, 0) AS order_count
    FROM dedup_sessions s
    LEFT JOIN page_views p ON s.session_id = p.session_id
    LEFT JOIN item_views i ON s.session_id = i.session_id
    LEFT JOIN orders o ON s.session_id = o.session_id
)

SELECT * FROM final