

with goals as (
    select
        date_trunc('year', start_at) as year_start,
        target_amount_home_currency,
        won_amount_home_currency
    from "warehouse"."marts_marts"."mart_sales_goals_vs_actual"
),

yearly as (
    select
        year_start,
        sum(target_amount_home_currency) as target_revenue_amount,
        sum(won_amount_home_currency)    as actual_revenue_amount
    from goals
    group by year_start
),

final as (
    select
        year_start,
        target_revenue_amount,
        actual_revenue_amount,
        actual_revenue_amount - target_revenue_amount as revenue_diff_vs_target,
        case
            when target_revenue_amount is not null
                 and target_revenue_amount <> 0
            then actual_revenue_amount::float / target_revenue_amount::float
            else null
        end as revenue_attainment_ratio
    from yearly
)

select *
from final