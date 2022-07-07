with pageviews_source as (
    select *
    from {{ source('web_tracking', 'pageviews')}}
),

renamed_pageviews as (
    select 
        * except(id),
        id as pageview_id
    from pageviews_source
)

select * from renamed_pageviews