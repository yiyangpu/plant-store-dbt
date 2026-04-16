SELECT
    ORDER_ID,
    SESSION_ID,
    CLIENT_NAME,
    PAYMENT_INFO,
    PAYMENT_METHOD,
    PHONE,
    SHIPPING_ADDRESS,
    SHIPPING_COST,
    STATE,
    TAX_RATE,
    ORDER_AT
FROM {{ source('web', 'orders') }}
