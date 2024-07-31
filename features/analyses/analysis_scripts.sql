
-- insert new row in users table
{% set user_id = 110 %}
{% set email_verified = 0 %}

INSERT INTO {{ source('features_sources', 'users') }} (user_id, created_date, updated_at, email_verified)
VALUES ({{ user_id }}, CURRENT_DATE, CURRENT_DATE, {{ email_verified }});

-- update user row 
{% set user_id_to_update = '100' %}

UPDATE {{ source('features_sources', 'users') }}
SET email_verified = CASE
    WHEN email_verified = 1 THEN 0
    WHEN email_verified = 0 THEN 1
    ELSE email_verified
END
WHERE user_id = {{ user_id_to_update }};
