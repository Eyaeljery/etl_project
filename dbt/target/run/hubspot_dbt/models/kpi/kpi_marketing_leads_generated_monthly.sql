
  create view "warehouse"."marts_kpi"."kpi_marketing_leads_generated_monthly__dbt_tmp"
    
    
  as (
    

with leads as (
    select
        date_trunc('month', became_lead_at) as month_start,
        contact_pk
    from "warehouse"."marts_marts"."mart_marketing_leads"
    where became_lead_at is not null
),

monthly as (
    select
        month_start,
        count(distinct contact_pk) as leads_count
    from leads
    group by month_start
),

with_py as (
    select
        m.month_start,
        m.leads_count,
        py.leads_count as leads_count_prev_year
    from monthly m
    left join monthly py
        on m.month_start = py.month_start + interval '1 year'
),

final as (
    select
        month_start,
        leads_count,
        leads_count_prev_year,
        leads_count - leads_count_prev_year as leads_diff_vs_py,
        case
            when leads_count_prev_year is not null
                 and leads_count_prev_year <> 0
            then (leads_count::float - leads_count_prev_year::float) / leads_count_prev_year::float
            else null
        end as leads_diff_ratio_vs_py
    from with_py
)

select *
from final
  );