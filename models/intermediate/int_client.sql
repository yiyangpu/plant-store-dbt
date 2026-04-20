WITH latest_session AS (
    SELECT
        client_id,
        device_os,
        session_time,
        ROW_NUMBER() OVER (
            PARTITION BY client_id
            ORDER BY session_time DESC
        ) AS rn
    FROM {{ ref('int_session') }}
    WHERE client_id IS NOT NULL
),

client_summary AS (
    SELECT
        client_id,
        MIN(session_time) AS first_session_time,
        MAX(session_time) AS last_session_time,
        COUNT(*) AS total_sessions,
        CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END AS is_returning_client
    FROM {{ ref('int_session') }}
    WHERE client_id IS NOT NULL
    GROUP BY client_id
)

SELECT
    s.client_id,
    l.device_os AS latest_device_os,
    s.first_session_time,
    s.last_session_time,
    s.total_sessions,
    s.is_returning_client
FROM client_summary s
LEFT JOIN latest_session l
    ON s.client_id = l.client_id
   AND l.rn = 1