with source as (
    select * from {{ source('coffee_shop', 'product_prices')}}
)

select * from source