{{ config(
    materialized='table'
) }}

with customer_orders as (
 select
   customers.id as customer_id,
   customers.name,
   customers.email,
   coalesce(count(orders.id),0) as number_of_orders,
   min(created_at) as first_order_at,
   max(created_at) as last_order_at
 from {{ source('coffee_shop', 'customers') }} customers 
 left join {{ source('coffee_shop', 'orders') }} orders  
   on customers.id = orders.customer_id
 group by customers.id, customers.name, customers.email
)
 
 
select 
  customer_orders.*,
  case 
    when number_of_orders > 1 then 'returned'
    else 'new'
    end as customer_type
from customer_orders
order by first_order_at