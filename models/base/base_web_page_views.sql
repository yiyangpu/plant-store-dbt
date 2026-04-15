SELECT
    SESSION_ID,
    PAGE_NAME,
    VIEW_AT
FROM {{ source('web', 'page_views') }}