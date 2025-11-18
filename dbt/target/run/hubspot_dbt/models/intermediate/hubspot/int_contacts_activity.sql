
  create view "warehouse"."marts_intermediate"."int_contacts_activity__dbt_tmp"
    
    
  as (
    



with

-- Appels
calls_contacts as (
    select
        c.call_id::text                         as engagement_id,
        'CALL'                                  as engagement_type,
        cast(c.call_timestamp as timestamp)     as activity_timestamp,
        c.owner_id,
        cast(value as text)                     as contact_id
    from "warehouse"."marts_staging"."stg_engagement_calls" c
    cross join lateral jsonb_array_elements_text(c.contacts_json::jsonb) as value
    where c.contacts_json is not null
),

-- Emails
emails_contacts as (
    select
        e.email_id::text                        as engagement_id,
        'EMAIL'                                 as engagement_type,
        cast(e.email_timestamp as timestamp)    as activity_timestamp,
        e.owner_id,
        cast(value as text)                     as contact_id
    from "warehouse"."marts_staging"."stg_engagement_emails" e
    cross join lateral jsonb_array_elements_text(e.contacts_json::jsonb) as value
    where e.contacts_json is not null
),

-- Tasks
tasks_contacts as (
    select
        t.task_id::text                         as engagement_id,
        'TASK'                                  as engagement_type,
        cast(t.task_timestamp as timestamp)     as activity_timestamp,
        t.owner_id,
        cast(value as text)                     as contact_id,
        t.is_completed::boolean                 as is_completed
    from "warehouse"."marts_staging"."stg_engagements_tasks" t
    cross join lateral jsonb_array_elements_text(t.contacts_json::jsonb) as value
    where t.contacts_json is not null
),

-- Meetings (via table de bridge normalisée)
meetings_contacts as (
    select
        m.meeting_id::text                      as engagement_id,
        'MEETING'                               as engagement_type,
        cast(m.meeting_timestamp as timestamp)  as activity_timestamp,
        m.owner_id,
        mc.contact_id::text                     as contact_id
    from "warehouse"."marts_staging"."stg_engagements_meetings" m
    join "warehouse"."marts_staging"."stg_meeting_contacts" mc
        on m.meeting_id = mc.meeting_id
),

-- Notes
notes_contacts as (
    select
        n.note_id::text                         as engagement_id,
        'NOTE'                                  as engagement_type,
        cast(n.created_at_raw as timestamp)     as activity_timestamp,
        n.owner_id,
        cast(value as text)                     as contact_id
    from "warehouse"."marts_staging"."stg_engagements_notes" n
    cross join lateral jsonb_array_elements_text(n.contacts_json::jsonb) as value
    where n.contacts_json is not null
),

unioned as (
    select
        engagement_id,
        engagement_type,
        activity_timestamp,
        owner_id,
        contact_id,
        null::boolean as is_completed
    from calls_contacts

    union all

    select
        engagement_id,
        engagement_type,
        activity_timestamp,
        owner_id,
        contact_id,
        null::boolean as is_completed
    from emails_contacts

    union all

    select
        engagement_id,
        engagement_type,
        activity_timestamp,
        owner_id,
        contact_id,
        is_completed
    from tasks_contacts

    union all

    select
        engagement_id,
        engagement_type,
        activity_timestamp,
        owner_id,
        contact_id,
        null::boolean as is_completed
    from meetings_contacts

    union all

    select
        engagement_id,
        engagement_type,
        activity_timestamp,
        owner_id,
        contact_id,
        null::boolean as is_completed
    from notes_contacts
),



dedup as (
    select
        unioned.*,
        row_number() over (
            partition by engagement_id, engagement_type, contact_id
            order by activity_timestamp desc
        ) as rn
    from unioned
),

clean as (
    select *
    from dedup
    where rn = 1
),



agg as (
    select
        contact_id,

        min(activity_timestamp)                 as first_engagement_at,
        max(activity_timestamp)                 as last_engagement_at,
        count(*)                                as total_engagements,

        count(*) filter (where engagement_type = 'CALL')    as calls_count,
        count(*) filter (where engagement_type = 'EMAIL')   as emails_count,
        count(*) filter (where engagement_type = 'TASK')    as tasks_count,
        count(*) filter (where engagement_type = 'MEETING') as meetings_count,
        count(*) filter (where engagement_type = 'NOTE')    as notes_count,

        -- tâches complétées vs créées (utile pour le KPI "ratio tâches complétées / créées")
        count(*) filter (
            where engagement_type = 'TASK'
        ) as tasks_total_for_ratio,

        count(*) filter (
            where engagement_type = 'TASK'
              and is_completed = true
        ) as tasks_completed_for_ratio
    from clean
    group by contact_id
)

select
    a.*,
    -- flag : contact avec activité dans les 30 derniers jours
    case
        when last_engagement_at >= (current_timestamp - interval '30 day')
            then true
        else false
    end as is_active_last_30d
from agg a
  );