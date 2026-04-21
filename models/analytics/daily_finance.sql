with daily_order_metrics as (

    select
        cast(order_at as date) as finance_date,
        count(distinct order_id) as order_count,
        round(sum(gross_item_revenue), 2) as gross_item_revenue,
        round(sum(shipping_cost_amount), 2) as shipping_revenue,
        round(sum(total_order_amount), 2) as gross_order_revenue,
        round(sum(
            case 
                when is_refunded then total_order_amount
                else 0
            end
        ), 2) as refund_amount,
        sum(is_refunded_flag) as refunded_order_count,
        round(sum(net_order_amount), 2) as net_order_revenue,
        round(sum(gross_item_revenue * tax_rate), 2) as tax_amount
    from {{ ref('fact_order') }}
    group by 1

),

employee_spans as (

    select
        employee_id,
        hire_date,
        coalesce(quit_date, current_date) as quit_date,
        annual_salary / 365.0 as daily_salary_cost
    from {{ ref('dim_employee') }}

),

calendar as (

    select cast(finance_date as date) as finance_date from daily_order_metrics
    union
    select cast(expense_date as date) as finance_date from {{ ref('int_expenses') }}
    union
    select cast(hire_date as date) as finance_date from employee_spans
    union
    select cast(quit_date as date) as finance_date from employee_spans

),

daily_salary_metrics as (

    select
        c.finance_date,
        round(coalesce(sum(e.daily_salary_cost), 0), 2) as salary_cost
    from calendar c
    left join employee_spans e
        on c.finance_date between e.hire_date and e.quit_date
    group by 1

),

final as (

    select
        c.finance_date,

        coalesce(o.order_count, 0) as order_count,

        round(coalesce(o.gross_item_revenue, 0), 2) as gross_item_revenue,
        round(coalesce(o.shipping_revenue, 0), 2) as shipping_revenue,
        round(coalesce(o.gross_order_revenue, 0), 2) as gross_order_revenue,
        round(coalesce(o.refund_amount, 0), 2) as refund_amount,
        coalesce(o.refunded_order_count, 0) as refunded_order_count,
        round(coalesce(o.net_order_revenue, 0), 2) as net_order_revenue,
        round(coalesce(o.tax_amount, 0), 2) as tax_amount,

        round(coalesce(e.hr_cost, 0), 2) as hr_cost,
        round(coalesce(s.salary_cost, 0), 2) as salary_cost,
        round(coalesce(e.tech_cost, 0), 2) as tech_cost,
        round(coalesce(e.warehouse_cost, 0), 2) as warehouse_cost,
        round(coalesce(e.other_cost, 0), 2) as other_cost,

        round(
            coalesce(e.hr_cost, 0)
            + coalesce(s.salary_cost, 0)
            + coalesce(e.tech_cost, 0)
            + coalesce(e.warehouse_cost, 0)
            + coalesce(e.other_cost, 0)
        , 2) as total_cost,

        round(
            coalesce(o.net_order_revenue, 0)
            - (
                coalesce(e.hr_cost, 0)
                + coalesce(s.salary_cost, 0)
                + coalesce(e.tech_cost, 0)
                + coalesce(e.warehouse_cost, 0)
                + coalesce(e.other_cost, 0)
            )
        , 2) as operating_profit,

        round(
            coalesce(o.net_order_revenue, 0)
            - (
                coalesce(e.hr_cost, 0)
                + coalesce(s.salary_cost, 0)
                + coalesce(e.tech_cost, 0)
                + coalesce(e.warehouse_cost, 0)
                + coalesce(e.other_cost, 0)
            )
            - coalesce(o.tax_amount, 0)
        , 2) as operating_profit_after_tax

    from calendar c
    left join daily_order_metrics o
        on c.finance_date = o.finance_date
    left join {{ ref('int_expenses') }} e
        on c.finance_date = e.expense_date
    left join daily_salary_metrics s
        on c.finance_date = s.finance_date

)

select *
from final
order by finance_date asc