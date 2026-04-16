SELECT
    "_fivetran_id" AS _FIVETRAN_ID,
    SESSION_ID,
    ITEM_NAME,
    ITEM_VIEW_AT AS ITEM_VIEW_TIME,
    PRICE_PER_UNIT,
    ADD_TO_CART_QUANTITY,
    REMOVE_FROM_CART_QUANTITY,
    "_fivetran_deleted" AS _FIVETRAN_DELETED,
    "_fivetran_synced" AS _FIVETRAN_SYNCED
FROM {{ source('web', 'item_views') }}