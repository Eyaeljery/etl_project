{{ config(
    materialized = 'view'
) }}

with leads as (
    select
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        contact_pk,
        is_lead,
        is_customer,
        became_lead_at,
        became_customer_at
    from {{ ref('mart_marketing_leads') }}
    where is_lead = true
),

agg as (
    select
        owner_id,
        owner_email,
        count(*) as total_leads,
        count(*) filter (where is_customer = true) as converted_leads
    from leads
    group by owner_id, owner_email
),

final as (
    select
        owner_id,
        owner_email,
        total_leads,
        converted_leads,
        case
            when total_leads > 0
            then converted_leads::float / total_leads::float
            else null
        end as win_rate
    from agg
)

select *
from final
order by win_rate desc