with last_events as (
  select 
    *,
    lag(timestamp, 1) 
    over (partition by visitor_id order by timestamp) as prior_page_visit,
  from {{ ref('stitched_pageviews') }}
),

time_diff as (
  select 
    *,
    date_diff(timestamp, prior_page_visit, second) as diff_in_seconds
  from last_events 
),

sessions as (
  select 
    *,
    case 
      when diff_in_seconds > (60 * 30) or prior_page_visit is null then 1 else 0 end as is_new_session
  from time_diff
)

select 
  customer_id,
  visitor_id,
  coalesce(visitor_id,"anon-user") || '-' || sum(is_new_session) over (partition by visitor_id order by timestamp) as session_id,
  timestamp,
  prior_page_visit,
  page,
  diff_in_seconds,
  is_new_session
from sessions
