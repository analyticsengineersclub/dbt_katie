with total_pageviews as (
  select * 
  from {{ ref('stg_pageviews')}}
),

add_visit_counts as (
  select 
    *, 
    count(visitor_id) over (partition by customer_id order by timestamp) as num_visit
  from total_pageviews
  where customer_id is not null 
),

stitched_id as (
  select 
  customer_id,
  visitor_id as first_visit_id
  from add_visit_counts 
  where 
    num_visit = 1 
)

select 
  total_pageviews.pageview_id,
  total_pageviews.customer_id,
  --coalesce(stitched_id.first_visit_id, total_pageviews.visitor_id) as visitor_id,
    case 
    when total_pageviews.customer_id is null then total_pageviews.visitor_id 
    else stitched_id.first_visit_id 
    end as visitor_id,
  total_pageviews.device_type,
  total_pageviews.timestamp,
  total_pageviews.page
from total_pageviews
inner join stitched_id 
    on total_pageviews.customer_id = stitched_id.customer_id 
