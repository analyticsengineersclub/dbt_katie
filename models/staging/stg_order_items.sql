with order_items_source as (
    select * from {{ source('coffee_shop', 'order_items')}}
),

order_items_renamed as (
    select 
        id as order_item_id,
        order_id,
        product_id 
    from order_items_source
)

select * from order_items_renamed 