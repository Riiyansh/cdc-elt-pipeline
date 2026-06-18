/*
    Collapse the append-only orders CDC log into current state.

    Handles:
      - Out-of-order / late-arriving events  → order by (ts_ms, lsn), keep latest
      - Soft deletes (tombstones, op='d')    → excluded from current state
      - Type inconsistency on `amount`        → try_cast (string or number → decimal)
      - Schema drift (`currency` appears late) → coalesce to default
      - Duplicate events                       → already deduped in raw load; window is safe regardless
*/

with raw as (
    select * from {{ source('raw', 'raw_orders_cdc') }}
),

-- primary key resolves from after (c/u) or before (d)
keyed as (
    select
        coalesce(
            json_extract_string(after_json, '$.order_id'),
            json_extract_string(before_json, '$.order_id')
        )::bigint as order_id,
        op,
        ts_ms,
        lsn,
        after_json
    from raw
),

-- latest event per order_id wins (ts first, then lsn as tiebreaker)
ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by ts_ms desc, lsn desc
        ) as _rn
    from keyed
),

latest as (
    select * from ranked where _rn = 1
)

select
    order_id,
    json_extract_string(after_json, '$.customer_id')::bigint            as customer_id,
    json_extract_string(after_json, '$.status')                        as status,
    try_cast(json_extract_string(after_json, '$.amount') as decimal(12,2)) as amount,
    coalesce(json_extract_string(after_json, '$.currency'), 'INR')      as currency,
    try_cast(json_extract_string(after_json, '$.created_at') as timestamp) as created_at,
    try_cast(json_extract_string(after_json, '$.updated_at') as timestamp) as updated_at,
    to_timestamp(ts_ms / 1000)                                         as _cdc_ts,
    op                                                                 as _last_op
from latest
where op != 'd'   -- drop tombstoned (deleted) orders
