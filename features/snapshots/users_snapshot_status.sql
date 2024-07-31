{% snapshot users_snapshot_check %}

    {% set new_schema = target.schema + '_snapshot' %}

    {{
        config(
          target_schema=new_schema,
          strategy='check',
          unique_key='user_id',
          check_cols=['email_verified'],
        )
    }}

    select * from {{ source('features_sources', 'users') }}

{% endsnapshot %}