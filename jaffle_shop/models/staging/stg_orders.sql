with source as (

    select * from {{ source('postgres_sources', 'orders') }}

),

renamed as (

    select
        order_id,
        user_id as customer_id,
        order_date,
        order_status

    from source

)

select * from renamed
