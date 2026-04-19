with orders as (
    select * from {{ ref('base_orders') }}
),

sessions as (
    select * from {{ ref('base_sessions') }}
),

client_orders as (
    select
        s.client_id,
        o.client_name,
        o.order_at,
        o.order_id,
        o.shipping_cost_amount,
        o.state
    from orders o
    left join sessions s
        on o.session_id = s.session_id
    where s.client_id is not null
),

aggregated as (
    select
        client_id,
        min(order_at) as first_order_at,
        max(order_at) as last_order_at,
        count(distinct order_id) as total_orders,
        sum(shipping_cost_amount) as total_shipping_cost
    from client_orders
    group by client_id
),

latest_name as (
    select
        client_id,
        client_name
    from client_orders
    qualify row_number() over (partition by client_id order by order_at desc) = 1
)

select
    a.client_id,
    ln.client_name,
    a.first_order_at,
    a.last_order_at,
    a.total_orders,
    a.total_shipping_cost
from aggregated a
left join latest_name ln
    on a.client_id = ln.client_id