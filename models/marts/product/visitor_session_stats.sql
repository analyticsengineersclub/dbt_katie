with visitor_session_stats as (
  select 
    visitor_id,
    session_id,
    timestamp_diff(end_session, start_session, second) as total_session_time_seconds,
    count(page) as num_pages_viewed
  from {{ ref('sessions')}}
  group by 1,2,3
  order by 4 desc 
)

select * from visitor_session_stats 
