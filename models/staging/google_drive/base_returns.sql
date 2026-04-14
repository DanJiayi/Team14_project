{{ config(materialized='view') }}

with source as (
    select * from {{ source('google_drive', 'returns') }}
),

renamed as (
    select
        "ORDER_ID" as order_id,
        cast("RETURNED_AT" as date) as returned_at,
        
        -- convert yes/no to boolean
        case 
            when "IS_REFUNDED" = 'yes' then true 
            else false 
        end as is_refunded,
        
        "_FIVETRAN_SYNCED" as synced_at
    from source
)

select * from renamed