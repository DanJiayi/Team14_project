-- models/analytics/fact_order.sql

with orders as (
    select * from {{ ref('int_orders') }}
),

-- Aggregate item metrics by session to calculate revenue per order
order_item_revenue as (
    select
        session_id,
        any_value(item_name) as primary_item_name,
        sum(add_to_cart_quantity * price_per_unit) as total_item_revenue
    from {{ ref('base_item_views') }}
    group by 1
),

-- Deduplicate sessions to ensure a 1:1 grain between session and client
sessions_deduped as (
    select
        session_id,
        client_id
    from {{ ref('base_sessions') }}
    qualify row_number() over (partition by session_id order by session_at asc) = 1
),

final as (
    select
        o.order_id,
        o.session_id,
        s.client_id,
        md5(coalesce(cast(it.primary_item_name as varchar), '')) as item_key,
        o.order_at,
        o.state,
        
        coalesce(it.total_item_revenue, 0) as gross_item_revenue,
        o.shipping_cost_amount,
        o.tax_rate,
        (coalesce(it.total_item_revenue, 0) + o.shipping_cost_amount) as total_order_amount,
        
        o.is_refunded,
        case when o.is_refunded then 1 else 0 end as is_refunded_flag,
        
        case 
            when o.is_refunded then 0 
            else (coalesce(it.total_item_revenue, 0) + o.shipping_cost_amount) 
        end as net_order_amount

    from orders o
    left join sessions_deduped s
        on o.session_id = s.session_id
    left join order_item_revenue it
        on o.session_id = it.session_id
)

select * from final