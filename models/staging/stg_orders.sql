with orders_source as (
    select * from {{ source('coffee_shop', 'orders')}}
),

renamed_orders as (
    select 
        id as order_id,
        created_at as order_created_at,
        customer_id,
        total as order_total,
        address,
        state,
        zip
    from orders_source
)

select * from renamed_orders
