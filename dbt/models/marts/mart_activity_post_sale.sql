{{ config(
    materialized = 'view'
) }}

with customers as (
    select
        contact_pk,
        contact_id,
        email,
        full_name,
        lifecycle_stage,
        is_customer,
        became_customer_at,
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name
    from {{ ref('mart_marketing_leads') }}
    where is_customer = true
      and became_customer_at is not null
),

calls_contacts as (
    select
        c.call_id::text as engagement_id,
        'CALL' as engagement_type,
        cast(c.call_timestamp as timestamp) as activity_timestamp,
        c.owner_id,
        cast(value as text) as contact_id,
        null::boolean as is_completed
    from {{ ref('stg_engagement_calls') }} c
    cross join lateral jsonb_array_elements_text(c.contacts_json::jsonb) as value
    where c.contacts_json is not null
),

emails_contacts as (
    select
        e.email_id::text as engagement_id,
        'EMAIL' as engagement_type,
        cast(e.email_timestamp as timestamp) as activity_timestamp,
        e.owner_id,
        cast(value as text) as contact_id,
        null::boolean as is_completed
    from {{ ref('stg_engagement_emails') }} e
    cross join lateral jsonb_array_elements_text(e.contacts_json::jsonb) as value
    where e.contacts_json is not null
),

tasks_contacts as (
    select
        t.task_id::text as engagement_id,
        'TASK' as engagement_type,
        cast(t.task_timestamp as timestamp) as activity_timestamp,
        t.owner_id,
        cast(value as text) as contact_id,
        t.is_completed::boolean as is_completed
    from {{ ref('stg_engagements_tasks') }} t
    cross join lateral jsonb_array_elements_text(t.contacts_json::jsonb) as value
    where t.contacts_json is not null
),

meetings_contacts as (
    select
        m.meeting_id::text as engagement_id,
        'MEETING' as engagement_type,
        cast(m.meeting_timestamp as timestamp) as activity_timestamp,
        m.owner_id,
        mc.contact_id::text as contact_id,
        null::boolean as is_completed
    from {{ ref('stg_engagements_meetings') }} m
    join {{ ref('stg_meeting_contacts') }} mc
        on m.meeting_id = mc.meeting_id
),

notes_contacts as (
    select
        n.note_id::text as engagement_id,
        'NOTE' as engagement_type,
        cast(n.created_at_raw as timestamp) as activity_timestamp,
        n.owner_id,
        cast(value as text) as contact_id,
        null::boolean as is_completed
    from {{ ref('stg_engagements_notes') }} n
    cross join lateral jsonb_array_elements_text(n.contacts_json::jsonb) as value
    where n.contacts_json is not null
),

unioned_engagements as (
    select * from calls_contacts
    union all select * from emails_contacts
    union all select * from tasks_contacts
    union all select * from meetings_contacts
    union all select * from notes_contacts
),

engagements_dedup as (
    select
        unioned_engagements.*,
        row_number() over (
            partition by engagement_id, engagement_type, contact_id
            order by activity_timestamp desc
        ) as rn
    from unioned_engagements
),

engagements_clean as (
    select
        engagement_id,
        engagement_type,
        activity_timestamp,
        owner_id,
        contact_id,
        is_completed
    from engagements_dedup
    where rn = 1
),

post_sale_engagements as (
    select
        e.engagement_id,
        e.engagement_type,
        e.activity_timestamp,
        e.owner_id,
        e.contact_id,
        e.is_completed,
        c.contact_pk,
        c.email,
        c.full_name,
        c.lifecycle_stage,
        c.became_customer_at,
        c.owner_email as contact_owner_email,
        c.owner_first_name as contact_owner_first_name,
        c.owner_last_name as contact_owner_last_name
    from engagements_clean e
    join customers c
        on cast(c.contact_id as text) = e.contact_id
    where e.activity_timestamp >= c.became_customer_at
),

agg as (
    select
        p.owner_id,
        p.contact_owner_email as owner_email,
        p.contact_pk,
        p.contact_id,
        p.email as contact_email,
        p.full_name as contact_full_name,
        date_trunc('month', p.activity_timestamp) as activity_month,
        p.became_customer_at,
        count(*) as total_post_sale_activities,
        count(*) filter (where p.engagement_type = 'CALL') as calls_post_sale_count,
        count(*) filter (where p.engagement_type = 'EMAIL') as emails_post_sale_count,
        count(*) filter (where p.engagement_type = 'TASK') as tasks_post_sale_count,
        count(*) filter (where p.engagement_type = 'MEETING') as meetings_post_sale_count,
        count(*) filter (where p.engagement_type = 'NOTE') as notes_post_sale_count,
        count(*) filter (where p.engagement_type = 'TASK') as tasks_post_sale_total,
        count(*) filter (where p.engagement_type = 'TASK' and p.is_completed = true) as tasks_post_sale_completed
    from post_sale_engagements p
    group by
        p.owner_id,
        p.contact_owner_email,
        p.contact_pk,
        p.contact_id,
        p.email,
        p.full_name,
        date_trunc('month', p.activity_timestamp),
        p.became_customer_at
),

final as (
    select
        owner_id,
        owner_email,
        contact_pk,
        contact_id,
        contact_email,
        contact_full_name,
        activity_month,
        became_customer_at,
        total_post_sale_activities,
        calls_post_sale_count,
        emails_post_sale_count,
        tasks_post_sale_count,
        meetings_post_sale_count,
        notes_post_sale_count,
        tasks_post_sale_total,
        tasks_post_sale_completed,
        case
            when tasks_post_sale_total > 0
            then tasks_post_sale_completed::float / tasks_post_sale_total
            else null
        end as tasks_post_sale_completion_ratio
    from agg
)

select *
from final