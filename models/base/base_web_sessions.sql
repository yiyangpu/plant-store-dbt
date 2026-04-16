SELECT
    SESSION_ID, 
    CAST(CLIENT_ID AS STRING) AS CLIENT_ID,
    IP AS CLIENT_IP, 
    OS AS DEVICE_OS, 
    TO_TIMESTAMP(SESSION_AT) as SESSION_TIME,
    "_fivetran_id" AS _FIVETRAN_ID,
    "_fivetran_deleted" AS _FIVETRAN_DELETED,
    "_fivetran_synced" AS _FIVETRAN_SYNCED
FROM {{ source("web", "sessions") }}
