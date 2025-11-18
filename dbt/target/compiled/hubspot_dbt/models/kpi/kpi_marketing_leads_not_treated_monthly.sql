

with untreated_leads as (
    select
        contact_pk,
        contact_id,
        became_lead_at
    from "warehouse"."marts_kpi"."kpi_marketing_leads_not_treated"
    where is_untreated_lead = true
      and became_lead_at is not null
),

by_month as (
    select
        date_trunc('month', became_lead_at)::date as month_start,
        count(distinct contact_pk)               as untreated_leads_count
    from untreated_leads
    group by date_trunc('month', became_lead_at)
),

final as (
    select
        month_start,
        untreated_leads_count
    from by_month
    order by month_start
)

select *
from final