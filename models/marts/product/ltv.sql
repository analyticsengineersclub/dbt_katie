with orders as (
  select * from {{ ref('stg_orders') }}
),

weekly_orders as (
  select 
    customer_id,
    date_trunc(order_created_at, week) as date_week,
    sum(order_total) as weekly_spend 
  from orders 
  group by 1, 2
),


customer_first_order as (
  select 
    customer_id,
    min(date_week) as first_week
  from weekly_orders 
  group by 1 
),

customer_all_weeks as (
  select  
    customer_first_order.customer_id,
    customer_first_order.first_week,
    weekly_orders.date_week,
    date_diff(datetime(weekly_orders.date_week), datetime(customer_first_order.first_week), week) as week_num
  from customer_first_order
  inner join weekly_orders 
    on customer_first_order.first_week <= weekly_orders.date_week
),

joined as (
  select 
    * except(weekly_spend),
    coalesce(weekly_spend, 0) as weekly_spend,
    sum(weekly_spend) over(
      partition by customer_id 
      order by date_week
      rows between unbounded preceding and current row) as total_spend
  from customer_all_weeks
  left join weekly_orders
    using(customer_id, date_week)
)

select * from joined 