{{ config(materialized='view') }}

with source as (
    select * from {{ source('web', 'orders') }}
),

renamed as (
    select
        "ORDER_ID" as order_id,
        "SESSION_ID" as session_id,
        "CLIENT_NAME" as client_name,
        "PHONE" as phone,
        "SHIPPING_ADDRESS" as shipping_address,
        "STATE" as state,
        
        -- handle 'USD 20' format
        cast(split_part("SHIPPING_COST", ' ', 2) as float) as shipping_cost_amount,
        
        "TAX_RATE" as tax_rate,
        "PAYMENT_INFO" as payment_info,
        "PAYMENT_METHOD" as payment_method,
        cast("ORDER_AT" as timestamp) as order_at
        
    from source
    where "_fivetran_deleted" = false
)

select * from renamed