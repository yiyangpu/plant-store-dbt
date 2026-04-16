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
    ORDER_AT AS ORDER_TIME
FROM {{ source('web', 'orders') }}
