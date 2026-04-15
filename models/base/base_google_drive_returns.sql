SELECT
    ORDER_ID,
    RETURNED_AT,
    IS_REFUNDED

FROM {{ source('google_drive', 'returns') }}