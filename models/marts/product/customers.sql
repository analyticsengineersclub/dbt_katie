{{ config(
    materialized='table'
) }}

with customer_orders as (
 select
   customers.customer_id,
   customers.name,
   customers.email,
   coalesce(count(orders.order_id),0) as number_of_orders,
   min(order_created_at) as first_order_at,
   max(order_created_at) as last_order_at,
   sum(orders.order_total) as total_dollars
 from {{ ref('stg_customers') }} customers 
 left join {{ ref('stg_orders') }} orders  
   on customers.customer_id = orders.customer_id
 group by customers.customer_id, customers.name, customers.email
),

final as (
  select 
    customer_orders.*,
    case 
      when number_of_orders > 1 then 'returned'
      else 'new'
      end as customer_type
  from customer_orders
  order by first_order_at
)

select * from final