SELECT
    SESSION_ID, 
    CLIENT_ID, 
    IP AS CLIENT_IP, 
    OS AS DEVICE_OS, 
    TO_TIMESTAMP(SESSION_AT) as SESSION_TIME
FROM {{ source("web", "sessions") }}
