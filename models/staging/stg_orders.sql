SELECT
    ORDER_ID,
    SESSION_ID,
    CLIENT_NAME,
    PAYMENT_METHOD,
    SHIPPING_ADDRESS,

    TO_TIMESTAMP(ORDER_AT) AS order_at

FROM {{ source('web', 'orders') }}