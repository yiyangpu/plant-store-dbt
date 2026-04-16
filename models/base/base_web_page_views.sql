SELECT
    SESSION_ID,
    PAGE_NAME,
    VIEW_AT AS VIEW_TIME,
    "_fivetran_id" AS _FIVETRAN_ID,
    "_fivetran_deleted" AS _FIVETRAN_DELETED,
    "_fivetran_synced" AS _FIVETRAN_SYNCED
FROM {{ source('web', 'page_views') }}