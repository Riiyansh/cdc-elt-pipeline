-- DQ gate: no order may be created in the future.
-- A common CDC corruption — clock skew or bad source timestamps.
-- Returns offending rows; a non-empty result fails the test.

select
    order_id,
    created_at
from {{ ref('fct_orders') }}
where created_at > current_timestamp
