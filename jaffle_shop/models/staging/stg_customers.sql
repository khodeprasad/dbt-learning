with source as (

    select * from {{ source('postgres_sources', 'customers') }}

),

renamed as (

    select
        customer_id, 
        first_name, 
        last_name
    from source

)

select * from renamed
