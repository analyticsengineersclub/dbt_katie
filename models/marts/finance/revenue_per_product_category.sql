with products as (
  select 
    product_id,
    product_category
  from {{ ref('stg_products') }}
),

orders as (
  select 
    orders.order_id,
    orders.order_created_at,
    orders.customer_id,
    orders.order_total,
    items.order_item_id,
    items.product_id,
    products.product_category 
  from {{ ref('stg_orders')}} orders
  inner join {{ ref('stg_order_items')}} items
    on orders.order_id = items.order_id 
  inner join products
    on items.product_id = products.product_id 
  order by orders.order_id

),

final as (
    select 
        date_trunc(date(order_created_at), day) as day,
        product_category,
        sum(order_total) as total_dollars
    from orders 
    group by 1,2 
    order by 1,2 
)

select * from final 