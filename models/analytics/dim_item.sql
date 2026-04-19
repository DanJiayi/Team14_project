-- models/analytics/dim_item.sql

with item_views as (
    select * from {{ ref('base_item_views') }}
),

-- Extract unique items and their respective unit prices
unique_items as (
    select distinct
        item_name,
        price_per_unit
    from item_views
)

select
    -- Generate surrogate key using MD5 hash of the item name
    md5(coalesce(cast(item_name as varchar), '')) as item_key,
    item_name,
    price_per_unit
from unique_items