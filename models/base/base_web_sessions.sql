select session_id, client_id, ip, os, to_timestamp(session_at) as session_at
from {{ source("web", "sessions") }}
