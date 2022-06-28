{{ config(
    materialized='table'
) }}

with orders as (
    select * from {{ ref('stg_orders')}}  
),

items as (
    select * from {{ ref('stg_order_items')}}  
),

products as (
    select * from {{ ref('stg_products')}}
),

prices as (
    select * from {{ ref('stg_product_prices')}}  
),

final as (
    select 
        orders.order_created_at,
        orders.order_id,
        items.product_id,
        products.product_name,
        products.product_category,
        prices.price
    from orders
    inner join items 
        on orders.order_id = items.order_id 
    inner join prices 
        on items.product_id = prices.product_id
    inner join products 
        on prices.product_id = products.product_id 
    where 
        prices.ended_at >= orders.order_created_at
    group by 1,2,3,4,5,6
)

select * from final 
