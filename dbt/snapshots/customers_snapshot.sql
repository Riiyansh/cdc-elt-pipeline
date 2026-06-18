{#
    SCD Type-2 snapshot of customers.
    Tracks tier changes over time (bronze → silver → gold ...) with
    dbt_valid_from / dbt_valid_to history. This is how you answer
    "what tier was this customer when they placed that order?".
#}

{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['tier', 'country', 'email']
    )
}}

select
    customer_id,
    full_name,
    email,
    country,
    tier,
    updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
