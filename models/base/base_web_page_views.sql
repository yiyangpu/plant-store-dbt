SELECT
    SESSION_ID,
    PAGE_NAME,
    VIEW_AT AS VIEW_TIME
FROM {{ source('web', 'page_views') }}