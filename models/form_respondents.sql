{{ config(
    materialized='incremental',
    unique_key='github_username'
) }}

with events as (
    select * 
    from {{ source('advanced_dbt_examples', 'form_events') }}
    {% if is_incremental() %}
     where timestamp >= (select date_add(max(last_form_entry), interval -1 hour) from {{ this }})
    {% endif %}
),

aggregated as (
    select
        github_username,
        min(timestamp) as first_form_entry,
        max(timestamp) as last_form_entry,
        count(*) as number_of_entries
    from events
    group by 1
)

select * from aggregated
