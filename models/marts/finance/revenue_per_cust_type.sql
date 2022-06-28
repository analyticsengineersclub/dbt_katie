with customers as (
  select 
    customer_id,
    customer_type 
  from {{ ref('customers') }}
),

orders as (
  select * from {{ ref('stg_orders') }}
),

final as (

  select 
    date_trunc(date(orders.order_created_at), day) as day,
    customers.customer_type,
    sum(orders.order_total) as total_dollars,
  from orders 
  left join customers 
    on orders.customer_id = customers.customer_id 
  group by 1, 2
  order by 1 
)

select * from final
