/*
    Collapse the customers CDC log into current state.
    Same latest-event-wins logic as orders. `email` may be null (null-flood
    injection) — we keep the row but the DQ tests will surface the null rate.
*/

with raw as (
    select * from {{ source('raw', 'raw_customers_cdc') }}
),

keyed as (
    select
        coalesce(
            json_extract_string(after_json, '$.customer_id'),
            json_extract_string(before_json, '$.customer_id')
        )::bigint as customer_id,
        op,
        ts_ms,
        lsn,
        after_json
    from raw
),

ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by ts_ms desc, lsn desc
        ) as _rn
    from keyed
),

latest as (
    select * from ranked where _rn = 1
)

select
    customer_id,
    json_extract_string(after_json, '$.full_name')                     as full_name,
    json_extract_string(after_json, '$.email')                         as email,
    json_extract_string(after_json, '$.country')                       as country,
    json_extract_string(after_json, '$.tier')                          as tier,
    try_cast(json_extract_string(after_json, '$.created_at') as timestamp) as created_at,
    try_cast(json_extract_string(after_json, '$.updated_at') as timestamp) as updated_at,
    to_timestamp(ts_ms / 1000)                                         as _cdc_ts
from latest
where op != 'd'
