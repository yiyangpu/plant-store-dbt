SELECT
    session_id, 
    client_id, 
    ip, 
    os, 
    to_timestamp(session_at) as session_at
FROM {{ source("web", "sessions") }}
