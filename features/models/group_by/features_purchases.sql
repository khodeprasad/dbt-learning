{# Define the list of day ranges #}
{% set days = [3, 15, 30, 180] %}

with source as (
    select * from {{ source('features_sources', 'purchases') }}
),

renamed as (
    select 
        user_id,
        purchase_price,
        "date" as purchase_date
    from source
),

aggregated as (
    select
        user_id,
        
        sum(purchase_price) as purchases_price_total, 
        count(purchase_price) as purchases_price_count,
        round(avg(purchase_price), 2) as purchases_price_avg,

        {# Generate the sum columns dynamically #}
        {% for day in days %}
            sum(case when purchase_date >= current_date - interval '{{ day }}' day then purchase_price else 0 end) as purchases_price_sum_{{ day }}d,
            count(case when purchase_date >= current_date - interval '{{ day }}' day then purchase_price else null end) as purchases_price_count_{{ day }}d,
            round(avg(case when purchase_date >= current_date - interval '{{ day }}' day then purchase_price else null end), 2) as purchases_price_avg_{{ day }}d,
            array_agg(purchase_price order by purchase_date desc) filter (where purchase_date >= current_date - interval '{{ day }}' day) as purchases_price_last_{{ day }}d{% if not loop.last %},{% endif %}
        {% endfor %}

    from renamed
    group by user_id
)

select * from aggregated
