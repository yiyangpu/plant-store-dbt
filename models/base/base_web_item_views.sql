SELECT
    SESSION_ID,
    ITEM_NAME,
    ITEM_VIEW_AT,
    PRICE_PER_UNIT,

    ADD_TO_CART_QUANTITY,
    REMOVE_FROM_CART_QUANTITY

FROM {{ source('web', 'item_views') }}