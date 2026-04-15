SELECT
    "_fivetran_id" AS _fivetran_id,
    SESSION_ID AS SESSION_ID,
    CLIENT_ID AS CLIENT_ID,
    OS AS OS,
    IP AS IP,
    CAST(SESSION_AT AS TIMESTAMP) AS SESSION_AT,
    "_fivetran_deleted" AS IS_DELETED,
    CAST("_fivetran_synced" AS TIMESTAMP) AS SYNCED_AT
FROM {{ source('web', 'sessions') }}

