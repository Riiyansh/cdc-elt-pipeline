-- DQ gate: orphan-order rate must stay under 5%.
-- Orphans = orders whose customer never appeared in the customer stream
-- (parent/child ordering hazard in CDC). A spike means the pipeline is
-- dropping or mis-joining parent events. Fails the build if breached.

with stats as (
    select
        sum(case when is_orphan_order then 1 else 0 end) as orphans,
        count(*)                                         as total
    from {{ ref('fct_orders') }}
)

select
    orphans,
    total,
    round(orphans * 100.0 / nullif(total, 0), 2) as orphan_pct
from stats
where orphans * 100.0 / nullif(total, 0) > 5.0
