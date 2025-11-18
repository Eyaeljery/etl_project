
  create view "warehouse"."marts_kpi"."kpi_marketing_top_forms_performance__dbt_tmp"
    
    
  as (
    

with submissions as (
    select
        form_id,
        email,
        conversion_id
    from "warehouse"."marts_staging"."stg_form_submissions"
    where form_id is not null
),

contacts as (
    select
        email,
        is_lead,
        is_customer
    from "warehouse"."marts_marts"."mart_marketing_leads"
),

joined as (
    select
        s.form_id,
        s.conversion_id,
        s.email,
        c.is_lead,
        c.is_customer
    from submissions s
    left join contacts c
        on lower(s.email) = lower(c.email)
),

agg as (
    select
        form_id,
        count(distinct conversion_id)                   as submissions_count,
        count(*) filter (where is_lead = true)          as leads_count,
        count(*) filter (where is_customer = true)      as customers_count
    from joined
    group by form_id
),

final as (
    select
        form_id,
        submissions_count,
        leads_count,
        customers_count,
        case 
            when submissions_count > 0 
            then leads_count::float / submissions_count::float
            else null
        end as lead_rate,
        case 
            when submissions_count > 0
            then customers_count::float / submissions_count::float
            else null
        end as customer_rate
    from agg
)

select *
from final
  );