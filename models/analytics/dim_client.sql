with clients as (
    select * from {{ ref('int_client') }}
)

select
    client_id,
    client_name,
    first_order_at,
    last_order_at,
    total_orders,
    total_shipping_cost,
    datediff('day', first_order_at, last_order_at) as customer_lifespan_days
from clients