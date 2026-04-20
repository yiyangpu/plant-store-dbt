SELECT
    item_name,
    avg_price,
    total_item_views,
    total_add_to_cart,
    total_remove_from_cart
FROM {{ ref('int_item') }}