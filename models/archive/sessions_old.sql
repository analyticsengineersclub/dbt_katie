with pageviews as (
  select * from {{ ref('stg_pageviews')}}
),

last_events as (
  select 
    *,
    lag(timestamp, 1) 
    over (partition by visitor_id order by timestamp) as last_page_visit,
  from pageviews
),

view_time_diff as (
  select 
    *,
    timestamp_diff(timestamp, last_page_visit, second) as diff_in_seconds
  from last_events 
),

pageviews_with_session as (
  select 
    *,
    case 
      when diff_in_seconds > (60 * 30) or last_page_visit is null then 1 else 0 end as is_new_session
  from view_time_diff
),

create_session_id as (
  select  
    pageview_id,
    coalesce(customer_id,"anon-user") || '-' || sum(is_new_session) over (partition by visitor_id order by timestamp) as session_id
  from pageviews_with_session
),

final as (
  select 
    pageviews_with_session.pageview_id,
    create_session_id.session_id,
    pageviews_with_session.visitor_id,
    pageviews_with_session.customer_id,
    pageviews_with_session.device_type,
    pageviews_with_session.timestamp,
    pageviews_with_session.page,
    pageviews_with_session.last_page_visit,
    pageviews_with_session.diff_in_seconds,
    pageviews_with_session.is_new_session,
    min(pageviews_with_session.timestamp) over (partition by pageviews_with_session.visitor_id, create_session_id.session_id) as start_session,
    max(pageviews_with_session.timestamp) over (partition by pageviews_with_session.visitor_id, create_session_id.session_id) as end_session
  from pageviews_with_session 
  inner join create_session_id 
    on pageviews_with_session.pageview_id = create_session_id.pageview_id
)

select * from final 


