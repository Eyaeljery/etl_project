

with deals as (
    select
        date_trunc('year', closed_at) as year_start,
        deal_status
    from "warehouse"."marts_marts"."mart_sales_deals"
    where deal_status in ('WON', 'LOST')
      and closed_at is not null
),

yearly as (
    select
        year_start,
        count(*) filter (where deal_status = 'WON')  as won_count,
        count(*) filter (where deal_status = 'LOST') as lost_count,
        count(*)                                     as total_considered
    from deals
    group by year_start
),

final as (
    select
        year_start,
        won_count,
        lost_count,
        total_considered,
        case
            when total_considered > 0
            then won_count::float / total_considered::float
            else null
        end as win_rate
    from yearly
)

select *
from final