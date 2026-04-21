with expenses as (

    select * from {{ ref('base_expenses') }}

),

daily_expenses as (

    select
        expense_date,

        sum(expense_amount) as total_expense,

        sum(case when lower(expense_type) = 'hr' then expense_amount else 0 end) as hr_cost,
        sum(case when lower(expense_type) = 'tech tool' then expense_amount else 0 end) as tech_cost,
        sum(case when lower(expense_type) = 'warehouse' then expense_amount else 0 end) as warehouse_cost,
        sum(case when lower(expense_type) = 'other' then expense_amount else 0 end) as other_cost

    from expenses
    group by 1

)

select *
from daily_expenses
order by expense_date asc