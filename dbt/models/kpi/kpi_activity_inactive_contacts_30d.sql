{{ config(
    materialized = 'view'
) }}

with contacts as (
    select
        contact_pk,
        contact_id,
        email,
        full_name
    from {{ ref('mart_marketing_leads') }}
),

activity as (
    select
        contact_id,
        coalesce(is_active_last_30d, false) as is_active_last_30d,
        total_engagements,
        first_engagement_at,
        last_engagement_at
    from {{ ref('int_contacts_activity') }}
),

joined as (
    select
        c.contact_pk,
        c.contact_id,
        c.email,
        c.full_name,
        a.is_active_last_30d,
        a.total_engagements,
        a.first_engagement_at,
        a.last_engagement_at
    from contacts c
    left join activity a
        on cast(c.contact_id as text) = cast(a.contact_id as text)
),

final as (
    select
        contact_pk,
        contact_id,
        email,
        full_name,
        coalesce(is_active_last_30d, false) as is_active_last_30d,
        case
            when coalesce(is_active_last_30d, false) = false
                then true
            else false
        end as is_inactive_last_30d,
        coalesce(total_engagements, 0) as total_engagements,
        first_engagement_at,
        last_engagement_at
    from joined
)

select *
from final