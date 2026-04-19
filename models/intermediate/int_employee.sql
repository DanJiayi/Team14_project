with joins as (
    select * from {{ ref('base_hr_joins') }}
),

quits as (
    select * from {{ ref('base_hr_quits') }}
),

employees as (
    select
        j.employee_id,
        j.name,
        j.title,
        j.hire_date,
        j.annual_salary,
        j.address,
        j.city,
        q.quit_date
    from joins j
    left join quits q
        on j.employee_id = q.employee_id
)

select * from employees