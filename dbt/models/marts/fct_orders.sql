/*
    Order fact table — INCREMENTAL.

    On incremental runs, only processes orders whose CDC event is newer than
    the max already loaded. This is what makes the pipeline cheap to run on a
    schedule: we don't re-scan the full history each time.
*/

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

select
    order_id,
    customer_id,
    is_orphan_order,
    country,
    tier,
    status,
    amount,
    currency,
    is_lost,
    is_completed,
    created_at,
    updated_at,
    _cdc_ts,
    current_timestamp as _processed_at
from {{ ref('int_orders_enriched') }}

{% if is_incremental() %}
where _cdc_ts > (select coalesce(max(_cdc_ts), timestamp '1900-01-01') from {{ this }})
{% endif %}
