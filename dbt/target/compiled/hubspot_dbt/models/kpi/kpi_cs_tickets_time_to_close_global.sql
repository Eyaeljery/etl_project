

with tickets as (
    select
        time_to_close_days
    from "warehouse"."marts_marts"."mart_cs_tickets"
    where is_closed = true
      and time_to_close_days is not null
),

agg as (
    select
        count(*)                as tickets_closed_count,
        avg(time_to_close_days) as avg_time_to_close_days
    from tickets
),

final as (
    select
        tickets_closed_count,
        avg_time_to_close_days
    from agg
)

select *
from final