with source as (
    select * from {{ source('google_drive', 'hr_joins') }}
),

cleaned as (
    select
        employee_id,
        name,
        title,
        replace(hire_date, 'day ', '')::date  as hire_date,
        annual_salary,
        address,
        city
    from source
)

select * from cleaned