/*
    Daily revenue aggregate — the serving layer the dashboard reads.
    Mirrors a Headout-style GMV / bookings report.
*/

with orders as (
    select * from {{ ref('fct_orders') }}
)

select
    cast(created_at as date)                                   as order_date,
    country,
    count(*)                                                   as total_orders,
    sum(case when is_completed then 1 else 0 end)              as completed_orders,
    sum(case when is_lost then 1 else 0 end)                   as lost_orders,
    sum(case when is_completed then amount else 0 end)         as gmv,
    round(
        sum(case when is_lost then 1 else 0 end) * 100.0
        / nullif(count(*), 0), 2)                              as cancellation_rate_pct
from orders
where created_at is not null
group by 1, 2
order by 1, 2
