{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='ignore',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    session_id,
    created_at,
    user_id,
    event_name,
    event_id

FROM {{ ref('stg_bingeflix__events') }}

{% if is_incremental() %}

{{ incremental_days_ago(  this , 'created_at', 1) }}

{% endif %}

