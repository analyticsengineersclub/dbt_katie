select 
  customers.customer_id,
  sum(orders.total) as total_dollars
from {{ ref('customers') }} customers
left join {{ source('coffee_shop', 'orders') }} as orders 
  on customers.customer_id = orders.customer_id 
group by 1 
order by 2 desc 