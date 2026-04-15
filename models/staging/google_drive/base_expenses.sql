with source as (

    select * from {{ source('google_drive', 'expenses') }}

),

cleaned as (

    select
        replace(_date_, '"', '')::date  as expense_date,
        replace(_expense_type_, '"', '')  as expense_type,
        replace(replace(_expense_amount_, '"', ''), '$ ', '')::number(10,2)  as expense_amount
    from source

)

select * from cleaned