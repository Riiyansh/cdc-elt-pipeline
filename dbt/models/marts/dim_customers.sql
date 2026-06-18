/*
    Customer dimension with lifetime order rollups.
*/

with customers as (
    select * from {{ ref('stg_customers') }}
),

order_stats as (
    select
        customer_id,
        count(*)                                  as lifetime_orders,
        sum(case when is_completed then amount else 0 end) as lifetime_gmv,
        sum(case when is_lost then 1 else 0 end)  as lost_orders,
        max(_cdc_ts)                              as last_order_event_at
    from {{ ref('int_orders_enriched') }}
    group by 1
)

select
    c.customer_id,
    c.full_name,
    c.email,
    c.country,
    c.tier,
    coalesce(os.lifetime_orders, 0)               as lifetime_orders,
    coalesce(os.lifetime_gmv, 0)                  as lifetime_gmv,
    coalesce(os.lost_orders, 0)                   as lost_orders,
    c.created_at,
    os.last_order_event_at
from customers c
left join order_stats os using (customer_id)
