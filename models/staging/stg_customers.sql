with customers_source as (
    select * from {{ source('coffee_shop', 'customers')}}
),

renamed_cust as (
    select
        id as customer_id,
        name,
        email
    from customers_source
)

select * from renamed_cust