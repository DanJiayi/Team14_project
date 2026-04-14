{{ config(materialized='view') }}

with source as (
    select * from {{ source('web', 'orders') }}
),

renamed as (
    select
        "ORDER_ID" as order_id,
        "SESSION_ID" as session_id,
        "CLIENT_NAME" as client_name,
        "STATE" as state,
        
        -- handle 'USD 20' format
        cast(split_part("SHIPPING_COST", ' ', 2) as float) as shipping_cost_amount,
        
        "TAX_RATE" as tax_rate,
        cast("ORDER_AT" as timestamp) as order_at,
        "PAYMENT_METHOD" as payment_method,
        
        -- metadata columns from fivetran
        "_fivetran_synced" as synced_at,
        "_fivetran_deleted" as is_deleted
    from source
)

select * from renamed