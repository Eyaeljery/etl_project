
  create view "warehouse"."marts_kpi"."kpi_marketing_conversion_by_campaign__dbt_tmp"
    
    
  as (
    

with contacts as (
    select
        contact_pk,
        analytics_source,
        is_lead,
        is_customer
    from "warehouse"."marts_marts"."mart_marketing_leads"
    where analytics_source is not null
),

by_source as (
    select
        analytics_source                               as source,
        count(*)                                       as contacts_count,
        count(*) filter (where is_lead = true)         as leads_count,
        count(*) filter (where is_customer = true)     as customers_count
    from contacts
    group by analytics_source
),

final as (
    select
        source,
        contacts_count,
        leads_count,
        customers_count,
        case
            when leads_count > 0
            then customers_count::float / leads_count::float
            else null
        end as conversion_rate
    from by_source
)

select *
from final
  );