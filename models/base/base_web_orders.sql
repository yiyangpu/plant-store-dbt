SELECT
    "_fivetran_id" AS _FIVETRAN_ID,
    ORDER_ID,
    SESSION_ID,
    CLIENT_NAME,
    PAYMENT_INFO,
    PAYMENT_METHOD,
    PHONE,
    SHIPPING_ADDRESS,
    TRY_TO_NUMBER(REGEXP_REPLACE(SHIPPING_COST, '[^0-9.]', '')) AS shipping_cost,
    STATE,
    TAX_RATE,
    ORDER_AT AS ORDER_TIME,
    "_fivetran_deleted" AS _FIVETRAN_DELETED,
    "_fivetran_synced" AS _FIVETRAN_SYNCED
FROM {{ source('web', 'orders') }}
