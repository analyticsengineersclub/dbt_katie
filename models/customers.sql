{{ config(
    materialized='table'
) }}

with customer_orders as (
 select
   customers.id as customer_id,
   customers.name,
   customers.email,
   count(orders.id) as number_of_orders,
   min(created_at) as first_order_at
 --from `analytics-engineers-club.coffee_shop.customers` customers
 from {{ source('coffee_shop', 'customers') }} customers 
 --inner join `analytics-engineers-club.coffee_shop.orders` orders
  inner join {{ source('coffee_shop', 'orders') }} orders  
   on customers.id = orders.customer_id
 group by customers.id, customers.name, customers.email
)
 
 
select *
from customer_orders
order by first_order_at