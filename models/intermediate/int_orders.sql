-- models/intermediate/int_orders.sql

with orders as (
    select * from {{ ref('base_orders') }}
),

-- Aggregate returns to ensure 1:1 grain with orders
-- This prevents row multiplication when an order has multiple return status updates
returns_aggregated as (
    select
        order_id,
        max(returned_at) as returned_at,
        max(is_refunded::integer)::boolean as is_refunded
    from {{ ref('base_returns') }}
    group by 1
),

-- Deduplicate source orders based on the latest timestamp
deduped_orders as (
    select *
    from orders
    qualify row_number() over (partition by order_id order by order_at asc) = 1
),

-- Join orders with aggregated return data
joined as (
    select
        o.*,
        r.returned_at,
        coalesce(r.is_refunded, false) as is_refunded
    from deduped_orders o
    left join returns_aggregated r
        on o.order_id = r.order_id
)

select * from joined