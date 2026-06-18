/*
    Join orders to their customer and derive business flags.
    Orphan orders (customer_id with no matching customer — a real CDC hazard
    when child events arrive before parent) are kept but flagged.
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    o.order_id,
    o.customer_id,
    c.customer_id is null                       as is_orphan_order,
    c.country,
    c.tier,
    o.status,
    o.amount,
    o.currency,
    o.status in ('cancelled', 'refunded')       as is_lost,
    o.status = 'delivered'                       as is_completed,
    o.created_at,
    o.updated_at,
    o._cdc_ts
from orders o
left join customers c using (customer_id)
