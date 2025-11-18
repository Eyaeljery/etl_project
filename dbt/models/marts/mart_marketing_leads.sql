{{ config(
    materialized = 'view'
) }}

with contacts as (
    select
        contact_pk,
        contact_id,
        email,
        first_name,
        last_name,
        full_name,
        country,
        lifecycle_stage,
        lead_status,
        is_marketable,
        owner_id,

        created_at,
        updated_at,
        became_lead_at,
        became_opportunity_at,
        became_customer_at,

        analytics_source,
        analytics_first_url,
        analytics_first_referrer,
        num_page_views,
        num_visits,
        first_analytics_at,
        last_analytics_at,
        first_touch_campaign,
        last_touch_campaign,

        companies_json,

        owner_email,
        owner_first_name,
        owner_last_name,
        owner_is_archived,

        extracted_at
    from {{ ref('int_contacts_base') }}
),

activity as (
    select
        contact_id        as activity_contact_id,
        first_engagement_at,
        last_engagement_at,
        total_engagements,
        calls_count,
        emails_count,
        tasks_count,
        meetings_count,
        notes_count,
        tasks_total_for_ratio,
        tasks_completed_for_ratio,
        is_active_last_30d
    from {{ ref('int_contacts_activity') }}
),

joined as (
    select
        c.*,
        a.first_engagement_at,
        a.last_engagement_at,
        a.total_engagements,
        a.calls_count,
        a.emails_count,
        a.tasks_count,
        a.meetings_count,
        a.notes_count,
        a.tasks_total_for_ratio,
        a.tasks_completed_for_ratio,
        a.is_active_last_30d
    from contacts c
    left join activity a
        on cast(c.contact_id as text) = cast(a.activity_contact_id as text)
),

final as (
    select
        -- clé
        contact_pk,
        contact_id,

        -- identité
        email,
        first_name,
        last_name,
        full_name,
        country,

        -- owner
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        owner_is_archived,

        -- lifecycle & statuts
        lifecycle_stage,
        lead_status,
        is_marketable,

        created_at,
        date(created_at)                 as created_date,

        became_lead_at,
        date(became_lead_at)            as became_lead_date,

        became_opportunity_at,
        date(became_opportunity_at)     as became_opportunity_date,

        became_customer_at,
        date(became_customer_at)        as became_customer_date,

        -- flags utiles pour les KPIs
        case
            when became_lead_at is not null then true
            else false
        end as is_lead,

        case
            when became_customer_at is not null then true
            else false
        end as is_customer,

        -- attribution / campagnes
        analytics_source,
        analytics_first_url,
        analytics_first_referrer,
        num_page_views,
        num_visits,
        first_analytics_at,
        last_analytics_at,
        first_touch_campaign,
        last_touch_campaign,

        -- activité
        first_engagement_at,
        last_engagement_at,
        total_engagements,
        calls_count,
        emails_count,
        tasks_count,
        meetings_count,
        notes_count,
        tasks_total_for_ratio,
        tasks_completed_for_ratio,
        is_active_last_30d,

        extracted_at
    from joined
)

select *
from final