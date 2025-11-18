
  create view "warehouse"."marts_kpi"."kpi_sales_revenue_yearly__dbt_tmp"
    
    
  as (
    

with won_deals as (
    select
        date_trunc('year', closed_at) as year_start,
        hs_closed_amount_in_home_currency as revenue
    from "warehouse"."marts_marts"."mart_sales_deals"
    where deal_status = 'WON'
      and closed_at is not null
),

yearly as (
    select
        year_start,
        sum(revenue) as revenue_amount
    from won_deals
    group by year_start
),

with_py as (
    select
        y.year_start,
        y.revenue_amount,
        py.revenue_amount as revenue_amount_prev_year
    from yearly y
    left join yearly py
        on y.year_start = py.year_start + interval '1 year'
),

final as (
    select
        year_start,
        revenue_amount,
        revenue_amount_prev_year,
        revenue_amount - revenue_amount_prev_year as revenue_diff_vs_py,
        case
            when revenue_amount_prev_year is not null
                 and revenue_amount_prev_year <> 0
            then (revenue_amount::float - revenue_amount_prev_year::float)
                 / revenue_amount_prev_year::float
            else null
        end as revenue_diff_ratio_vs_py
    from with_py
)

select *
from final
  );