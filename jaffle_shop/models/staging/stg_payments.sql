with source as (

    select * from {{ source('postgres_sources', 'payments') }}

),

renamed as (

    select
        payment_id,
        order_id,
        payment_method,

        -- `amount` is currently stored in cents, so we convert it to dollars
        payment_amount / 100 as payment_amount

    from source

)

select * from renamed
