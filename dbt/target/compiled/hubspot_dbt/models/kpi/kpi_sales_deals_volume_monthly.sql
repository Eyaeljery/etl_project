

with deals as (
    select
        date_trunc('month', created_at) as created_month_start,
        date_trunc('month', closed_at)  as closed_month_start,
        deal_status
    from "warehouse"."marts_marts"."mart_sales_deals"
),

created as (
    select
        created_month_start as month_start,
        count(*) as deals_created_count
    from deals
    where created_month_start is not null
    group by created_month_start
),

closed as (
    select
        closed_month_start as month_start,
        count(*) filter (where deal_status = 'WON')  as deals_won_count,
        count(*) filter (where deal_status = 'LOST') as deals_lost_count
    from deals
    where closed_month_start is not null
      and deal_status in ('WON', 'LOST')
    group by closed_month_start
),

combined as (
    select
        coalesce(c.month_start, cl.month_start) as month_start,
        coalesce(c.deals_created_count, 0)      as deals_created_count,
        coalesce(cl.deals_won_count, 0)         as deals_won_count,
        coalesce(cl.deals_lost_count, 0)        as deals_lost_count
    from created c
    full outer join closed cl
        on c.month_start = cl.month_start
),

final as (
    select
        month_start,
        deals_created_count,
        deals_won_count,
        deals_lost_count
    from combined
)

select *
from final