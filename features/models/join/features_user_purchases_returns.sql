{% set exclude_columns = ['user_id'] %}

with featrues_purchases as (

    select * from {{ ref('features_purchases') }}

),

features_returns as (

    select * from {{ ref('features_returns') }}

),

users as (

    select * from {{ source('features_sources', 'users') }}

),

final as (

    select
        users.*, 
        {{ select_columns_except('features_purchases', exclude_columns) }},
        {{ select_columns_except('features_returns', exclude_columns) }}

    from users

    left join featrues_purchases
        on users.user_id = featrues_purchases.user_id

    left join features_returns
        on users.user_id = features_returns.user_id

)

select * from final
