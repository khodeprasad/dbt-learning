{# Define the list of day ranges #}
{% set days = [3, 15, 30, 180] %}

with source as (
    select * from {{ source('features_sources', 'returns') }}
),

renamed as (
    select 
        user_id,
        refund_amt,
        "date" as return_date
    from source
),

aggregated as (
    select
        user_id,
        
        sum(refund_amt) as refund_amt_total, 
        count(refund_amt) as refund_amt_count,
        round(avg(refund_amt), 2) as refund_amt_avg,

        {# Generate the sum columns dynamically #}
        {% for day in days %}
            sum(case when return_date >= current_date - interval '{{ day }}' day then refund_amt else 0 end) as refund_amt_sum_{{ day }}d,
            count(case when return_date >= current_date - interval '{{ day }}' day then refund_amt else null end) as refund_amt_count_{{ day }}d,
            round(avg(case when return_date >= current_date - interval '{{ day }}' day then refund_amt else null end), 2) as refund_amt_avg_{{ day }}d,
            array_agg(refund_amt order by return_date desc) filter (where return_date >= current_date - interval '{{ day }}' day) as refund_amt_last_{{ day }}d{% if not loop.last %},{% endif %}
        {% endfor %}
        

    from renamed
    group by user_id
)

select * from aggregated
