with source as (
    select * from {{ source('google_drive', 'hr_quits') }}
),

cleaned as (
    select
        employee_id,
        quit_date
    from source
)

select * from cleaned