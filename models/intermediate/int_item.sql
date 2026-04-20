SELECT
    item_name,
    AVG(price_per_unit) AS avg_price,
    COUNT(*) AS total_item_views,
    SUM(add_to_cart_quantity) AS total_add_to_cart,
    SUM(remove_from_cart_quantity) AS total_remove_from_cart,

    COUNT(DISTINCT CASE WHEN add_to_cart_quantity > 0 THEN session_id END) * 1.0 / COUNT(*) AS conversion_rate,

FROM {{ ref('base_web_item_views') }}
GROUP BY item_name