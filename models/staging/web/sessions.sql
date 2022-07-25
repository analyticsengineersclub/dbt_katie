with pageviews as (
    select * from {{ ref('stg_pageviews')}}
),

previous_pageview as (
    select
        *,
        -- returns the value of the previous row (aka last pageview, partitioned by the blended user id from user stitching)
        lag(timestamp) over (partition by blended_user_id order by timestamp) as previous_pageview_at
    from pageviews
),

pageview_delta as (
    select
        *,
        -- time in minutes from the last page view to current page view 
        date_diff(timestamp, previous_pageview_at, minute) as minutes_since_previous_pageview
    from previous_pageview

),

session_marker as (
    select
        *,
        -- 30 minutes is standard for a session so if it's > 30 then it's a new session, if not it's the same 
        cast(coalesce(minutes_since_previous_pageview > 30, true) as integer) as is_new_session
    from pageview_delta
),

session_number as (
    select
        *,
        sum(is_new_session) over (
            partition by blended_user_id
            order by timestamp
            -- defines the window for the order by timestamp
            -- unbounded preceding starts at the first row, up to current row 
            rows between unbounded preceding and current row
        ) as session_number
    from session_marker
)

select * from session_number 
