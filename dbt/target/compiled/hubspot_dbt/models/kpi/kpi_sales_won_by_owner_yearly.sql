

with won_deals as (
    select
        date_trunc('year', closed_at) as year_start,
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        deal_id,
        hs_closed_amount_in_home_currency as amount
    from "warehouse"."marts_marts"."mart_sales_deals"
    where deal_status = 'WON'
      and closed_at is not null
),

yearly as (
    select
        year_start,
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        count(distinct deal_id) as won_deals_count,
        sum(amount) as won_revenue
    from won_deals
    group by
        year_start,
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name
),

final as (
    select
        year_start,
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        won_deals_count,
        won_revenue
    from yearly
)

select *
from final