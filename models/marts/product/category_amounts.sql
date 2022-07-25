
with order_items as (
  select
    * except(product_name, product_category),
    replace(product_name, " ", "_") as product_name,
    replace(product_category, " ", "_") as product_category
  from {{ ref('order_items')}}
)

select
  date_trunc(order_created_at, month) as date_month,
  {% for product_category in ['coffee_beans', 'merch', 'brewing_supples'] %}
sum(case when product_category = '{{product_category}}' then price end) as {{product_category}}_amount,
{% endfor %}
sum(price) as total_product_price
--   sum(case when product_category = 'coffee beans' then price end) as coffee_beans_amount,
--   sum(case when product_category = 'merch' then price end) as merch_amount,
--   sum(case when product_category = 'brewing supplies' then price end) as brewing_supplies_amount
from order_items
group by 1
order by 1 
