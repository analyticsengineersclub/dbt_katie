with products_source as (
    select * from {{ source('coffee_shop', 'products')}}
),

products_renamed as (
    select 
        id as product_id,
        name as product_name,
        category as product_category,
        created_at as product_created_at
    from products_source
)

select * from products_renamed 
