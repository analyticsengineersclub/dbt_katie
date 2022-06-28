select 
  date_trunc(date(order_created_at), day) as order_day,
  product_category,
  sum(price) as total_dollars
from {{ ref('order_items')}}
group by 1,2
order by 1,2
