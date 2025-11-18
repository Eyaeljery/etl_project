
  create view "warehouse"."marts_kpi"."kpi_marketing_enrollments_yearly__dbt_tmp"
    
    
  as (
    

with won_deals as (
    select
        date_trunc('year', closed_at) as year_start,
        deal_id
    from "warehouse"."marts_marts"."mart_sales_deals"
    where deal_status = 'WON'
      and closed_at is not null
),

yearly as (
    select
        year_start,
        count(distinct deal_id) as enrollments_count
    from won_deals
    group by year_start
),

with_py as (
    select
        y.year_start,
        y.enrollments_count,
        py.enrollments_count as enrollments_count_prev_year
    from yearly y
    left join yearly py
        on y.year_start = py.year_start + interval '1 year'
),

final as (
    select
        year_start,
        enrollments_count,
        enrollments_count_prev_year,
        enrollments_count - enrollments_count_prev_year as enrollments_diff_vs_py,
        case
            when enrollments_count_prev_year is not null
                 and enrollments_count_prev_year <> 0
            then (enrollments_count::float - enrollments_count_prev_year::float)
                 / enrollments_count_prev_year::float
            else null
        end as enrollments_diff_ratio_vs_py
    from with_py
)

select *
from final
  );