
  create view "warehouse"."marts_intermediate"."int_engagements_all__dbt_tmp"
    
    
  as (
    



with calls_src as (
    select
        *
    from "warehouse"."marts_staging"."stg_engagement_calls"
),

calls_dedup as (
    select
        calls_src.*,
        row_number() over (
            partition by call_id
            order by cast(updated_at_raw as timestamp) desc,
                     cast(extracted_at as timestamp) desc
        ) as rn
    from calls_src
),

calls_latest as (
    select *
    from calls_dedup
    where rn = 1
),

calls as (
    select
        call_id::text                              as engagement_id,
        'CALL'                                     as engagement_type,
        cast(created_at_raw as timestamp)          as created_at,
        cast(updated_at_raw as timestamp)          as updated_at,
        cast(call_timestamp as timestamp)          as activity_timestamp,
        date(cast(call_timestamp as timestamp))    as activity_date,
        owner_id,
        status,
        outcome,
        direction,
        duration_seconds,
        null::text                                 as subject,
        null::text                                 as body_text,
        null::text                                 as body_html,
        null::boolean                              as is_completed,
        contacts_json,
        companies_json,
        deals_json,
        tickets_json,
        cast(extracted_at as timestamp)            as extracted_at
    from calls_latest
),



emails_src as (
    select * from "warehouse"."marts_staging"."stg_engagement_emails"
),

emails_dedup as (
    select
        emails_src.*,
        row_number() over (
            partition by email_id
            order by cast(updated_at_raw as timestamp) desc,
                     cast(extracted_at as timestamp) desc
        ) as rn
    from emails_src
),

emails_latest as (
    select *
    from emails_dedup
    where rn = 1
),

emails as (
    select
        email_id::text                             as engagement_id,
        'EMAIL'                                    as engagement_type,
        cast(created_at_raw as timestamp)          as created_at,
        cast(updated_at_raw as timestamp)          as updated_at,
        cast(email_timestamp as timestamp)         as activity_timestamp,
        date(cast(email_timestamp as timestamp))   as activity_date,
        owner_id,
        status,
        null::text                                 as outcome,
        direction,
        null::double precision                     as duration_seconds,
        subject,
        null::text                                 as body_text,
        null::text                                 as body_html,
        null::boolean                              as is_completed,
        contacts_json,
        companies_json,
        deals_json,
        tickets_json,
        cast(extracted_at as timestamp)            as extracted_at
    from emails_latest
),



tasks_src as (
    select * from "warehouse"."marts_staging"."stg_engagements_tasks"
),

tasks_dedup as (
    select
        tasks_src.*,
        row_number() over (
            partition by task_id
            order by cast(updated_at_raw as timestamp) desc,
                     cast(extracted_at as timestamp) desc
        ) as rn
    from tasks_src
),

tasks_latest as (
    select *
    from tasks_dedup
    where rn = 1
),

tasks as (
    select
        task_id::text                               as engagement_id,
        'TASK'                                      as engagement_type,
        cast(created_at_raw as timestamp)           as created_at,
        cast(updated_at_raw as timestamp)           as updated_at,
        cast(task_timestamp as timestamp)           as activity_timestamp,
        date(cast(task_timestamp as timestamp))     as activity_date,
        owner_id,
        status,
        null::text                                  as outcome,
        null::text                                  as direction,
        null::double precision                      as duration_seconds,
        subject,
        null::text                                  as body_text,
        null::text                                  as body_html,
        is_completed::boolean                       as is_completed,
        contacts_json,
        companies_json,
        deals_json,
        tickets_json,
        cast(extracted_at as timestamp)             as extracted_at
    from tasks_latest
),



meetings_src as (
    select * from "warehouse"."marts_staging"."stg_engagements_meetings"
),

meetings_dedup as (
    select
        meetings_src.*,
        row_number() over (
            partition by meeting_id
            order by cast(updated_at_raw as timestamp) desc,
                     cast(extracted_at as timestamp) desc
        ) as rn
    from meetings_src
),

meetings_latest as (
    select *
    from meetings_dedup
    where rn = 1
),

meetings as (
    select
        meeting_id::text                            as engagement_id,
        'MEETING'                                   as engagement_type,
        cast(created_at_raw as timestamp)           as created_at,
        cast(updated_at_raw as timestamp)           as updated_at,
        cast(meeting_timestamp as timestamp)        as activity_timestamp,
        date(cast(meeting_timestamp as timestamp))  as activity_date,
        owner_id,
        null::text                                  as status,
        null::text                                  as outcome,
        null::text                                  as direction,
        extract(epoch from (
            cast(end_time as timestamp)
            - cast(start_time as timestamp)
        ))::double precision                        as duration_seconds,
        title                                       as subject,
        body                                        as body_text,
        null::text                                  as body_html,
        null::boolean                               as is_completed,
        contacts_json,
        companies_json,
        deals_json,
        tickets_json,
        cast(extracted_at as timestamp)             as extracted_at
    from meetings_latest
),



notes_src as (
    select * from "warehouse"."marts_staging"."stg_engagements_notes"
),

notes_dedup as (
    select
        notes_src.*,
        row_number() over (
            partition by note_id
            order by cast(updated_at_raw as timestamp) desc,
                     cast(extracted_at as timestamp) desc
        ) as rn
    from notes_src
),

notes_latest as (
    select *
    from notes_dedup
    where rn = 1
),

notes as (
    select
        note_id::text                               as engagement_id,
        'NOTE'                                      as engagement_type,
        cast(created_at_raw as timestamp)           as created_at,
        cast(updated_at_raw as timestamp)           as updated_at,
        cast(created_at_raw as timestamp)           as activity_timestamp,
        date(cast(created_at_raw as timestamp))     as activity_date,
        owner_id,
        null::text                                  as status,
        null::text                                  as outcome,
        null::text                                  as direction,
        null::double precision                      as duration_seconds,
        null::text                                  as subject,
        body_text,
        body_html,
        null::boolean                               as is_completed,
        contacts_json,
        companies_json,
        deals_json,
        tickets_json,
        cast(extracted_at as timestamp)             as extracted_at
    from notes_latest
),



unioned as (
    select * from calls
    union all
    select * from emails
    union all
    select * from tasks
    union all
    select * from meetings
    union all
    select * from notes
)

select
    u.*,
    o.email      as owner_email,
    o.first_name as owner_first_name,
    o.last_name  as owner_last_name,
    o.is_archived as owner_is_archived
from unioned u
left join "warehouse"."marts_intermediate"."int_owners" o
    on u.owner_id = o.owner_id
  );