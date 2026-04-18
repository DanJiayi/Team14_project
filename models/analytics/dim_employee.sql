with employees as (
    select * from {{ ref('int_employee') }}
)

select
    employee_id,
    name,
    title,
    hire_date,
    quit_date,
    annual_salary,
    address,
    city,
    case when quit_date is null then true else false end as is_active,
    datediff('day', hire_date, coalesce(quit_date, current_date())) as tenure_days
from employees