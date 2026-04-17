{{ config(materialized='table') }}

select
    SESSION_ID,
    CLIENT_ID,
    SESSION_AT,

    -- page features
    page_view_count,
    viewed_shop_page,
    viewed_cart_page,
    viewed_faq_page,
    viewed_plant_care_page,

    -- item features
    item_view_count,
    distinct_items_viewed,
    add_to_cart_count,
    remove_from_cart_count,
    removed_from_cart,
    order_count,
    
    -- funnel flags
    case when distinct_items_viewed > 0 then 1 else 0 end as viewed_item,
    added_to_cart,
    placed_order

from {{ ref('int_sessions') }}