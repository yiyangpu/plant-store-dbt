SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    _FIVETRAN_SYNCED,
    ORDER_ID,
    RETURNED_AT AS RETURNED_DATE,
    IS_REFUNDED
FROM {{ source('google_drive', 'returns') }}