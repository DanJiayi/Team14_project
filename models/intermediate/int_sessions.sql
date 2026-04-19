with sessions_dedup as (

    select
        SESSION_ID,
        CLIENT_ID,
        IP,
        OS,
        SESSION_AT
    from (

        select
            *,
            row_number() over (
                partition by SESSION_ID
                order by SESSION_AT asc
            ) as rn
        from {{ ref('base_sessions') }}

    )
    where rn = 1

)
, page_features as (

    select
        SESSION_ID,
        count(*) as page_view_count,
        max(case when lower(PAGE_NAME) like '%shop%' then 1 else 0 end) as viewed_shop_page,
        max(case when lower(PAGE_NAME) like '%cart%' then 1 else 0 end) as viewed_cart_page,
        max(case when lower(PAGE_NAME) like '%faq%' then 1 else 0 end) as viewed_faq_page,
        max(case when lower(PAGE_NAME) like '%plant%' then 1 else 0 end) as viewed_plant_care_page

    from {{ ref('base_page_views') }}
    group by 1

)

, item_features as (

    select
        SESSION_ID,
        count(*) as item_view_count,
        count(distinct ITEM_NAME) as distinct_items_viewed,
        coalesce(sum(ADD_TO_CART_QUANTITY), 0) as add_to_cart_count,
        coalesce(sum(REMOVE_FROM_CART_QUANTITY), 0) as remove_from_cart_count,
        max(case when ADD_TO_CART_QUANTITY > 0 then 1 else 0 end) as added_to_cart,
        max(case when REMOVE_FROM_CART_QUANTITY > 0 then 1 else 0 end) as removed_from_cart

    from {{ ref('base_item_views') }}
    group by 1

)

, order_features as (

    select
        SESSION_ID,
        count(distinct ORDER_ID) as order_count,
        max(case when ORDER_ID is not null then 1 else 0 end) as placed_order

    from {{ ref('base_orders') }}
    group by 1

)

select
    s.SESSION_ID,
    s.CLIENT_ID,
    s.IP,
    s.OS,
    s.SESSION_AT,

    coalesce(p.page_view_count, 0) as page_view_count,
    coalesce(p.viewed_shop_page, 0) as viewed_shop_page,
    coalesce(p.viewed_cart_page, 0) as viewed_cart_page,
    coalesce(p.viewed_faq_page, 0) as viewed_faq_page,
    coalesce(p.viewed_plant_care_page, 0) as viewed_plant_care_page,

    coalesce(i.item_view_count, 0) as item_view_count,
    coalesce(i.distinct_items_viewed, 0) as distinct_items_viewed,
    coalesce(i.add_to_cart_count, 0) as add_to_cart_count,
    coalesce(i.remove_from_cart_count, 0) as remove_from_cart_count,
    coalesce(i.added_to_cart, 0) as added_to_cart,
    coalesce(i.removed_from_cart, 0) as removed_from_cart,

    coalesce(o.order_count, 0) as order_count,
    coalesce(o.placed_order, 0) as placed_order

from sessions_dedup s
left join page_features p
    on s.SESSION_ID = p.SESSION_ID
left join item_features i
    on s.SESSION_ID = i.SESSION_ID
left join order_features o
on s.SESSION_ID = o.SESSION_ID