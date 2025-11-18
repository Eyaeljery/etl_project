

with leads as (
    select
        contact_pk,
        contact_id,
        became_lead_at
    from "warehouse"."marts_marts"."mart_marketing_leads"
    where became_lead_at is not null
),

activity as (
    select
        contact_id,
        coalesce(total_engagements, 0) as total_engagements
    from "warehouse"."marts_intermediate"."int_contacts_activity"
),

joined as (
    select
        l.contact_pk,
        l.contact_id,
        l.became_lead_at,
        a.total_engagements
    from leads l
    left join activity a
        on cast(l.contact_id as text) = cast(a.contact_id as text)
),

final as (
    select
        contact_pk,
        contact_id,
        became_lead_at,
        total_engagements,
        case
            when total_engagements = 0 or total_engagements is null
                then true
            else false
        end as is_untreated_lead
    from joined
)

select *
from final