{% snapshot users_snapshot_timestamp %}

    {% set new_schema = target.schema + '_snapshot' %}

    {{
        config(
          target_schema=new_schema,
          strategy='timestamp',
          unique_key='user_id',
          updated_at='updated_at',
        )
    }}

    select * from {{ source('features_sources', 'users') }}

{% endsnapshot %}