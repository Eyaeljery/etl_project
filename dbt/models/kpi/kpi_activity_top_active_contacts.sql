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
        coalesce(total_engagements, 0) as total_engagements,
        coalesce(calls_count, 0)       as calls_count,
        coalesce(emails_count, 0)      as emails_count,
        coalesce(tasks_count, 0)       as tasks_count,
        coalesce(meetings_count, 0)    as meetings_count,
        coalesce(notes_count, 0)       as notes_count,
        first_engagement_at,
        last_engagement_at,
        coalesce(is_active_last_30d, false) as is_active_last_30d
    from {{ ref('int_contacts_activity') }}
    -- 🔥⟵ FILTRE DES ANNÉES APPLIQUÉ ICI
    where last_engagement_at >= '2023-01-01'
),

joined as (
    select
        c.contact_pk,
        c.contact_id,
        c.email,
        c.full_name,
        a.total_engagements,
        a.calls_count,
        a.emails_count,
        a.tasks_count,
        a.meetings_count,
        a.notes_count,
        a.first_engagement_at,
        a.last_engagement_at,
        a.is_active_last_30d
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
        coalesce(total_engagements, 0) as total_engagements,
        coalesce(calls_count, 0)       as calls_count,
        coalesce(emails_count, 0)      as emails_count,
        coalesce(tasks_count, 0)       as tasks_count,
        coalesce(meetings_count, 0)    as meetings_count,
        coalesce(notes_count, 0)       as notes_count,
        first_engagement_at,
        last_engagement_at,
        coalesce(is_active_last_30d, false) as is_active_last_30d,
        row_number() over (
            order by coalesce(total_engagements, 0) desc
        ) as activity_rank
    from joined
)

select *
from final