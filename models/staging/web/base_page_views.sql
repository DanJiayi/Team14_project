SELECT
    "_fivetran_id" AS _fivetran_id,
    SESSION_ID AS SESSION_ID,
    PAGE_NAME AS PAGE_NAME,
    CAST(VIEW_AT AS TIMESTAMP) AS VIEW_AT,
    "_fivetran_deleted" AS IS_DELETED,
    CAST("_fivetran_synced" AS TIMESTAMP) AS SYNCED_AT
FROM {{ source('web', 'page_views') }}
