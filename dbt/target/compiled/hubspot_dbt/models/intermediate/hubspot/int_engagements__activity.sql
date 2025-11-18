

with calls as (
    select
        call_id                           as activity_id,
        'call'                            as activity_type,
        call_timestamp                    as activity_ts,
        date_trunc('month', call_timestamp)::date as activity_month,
        owner_id                          as owner_id,

        case
            when contacts_json::text ~ '\d+'
                then ((regexp_match(contacts_json::text, '(\d+)'))[1])::bigint
        end                               as contact_id,

        case
            when companies_json::text ~ '\d+'
                then ((regexp_match(companies_json::text, '(\d+)'))[1])::bigint
        end                               as company_id,

        null::bigint                      as deal_id,
        created_at_raw                    as created_at,
        updated_at_raw                    as updated_at,
        extracted_at
    from "warehouse"."marts_staging"."stg_engagement_calls"
    where call_timestamp is not null
),

emails as (
    select
        email_id                          as activity_id,
        'email'                           as activity_type,
        email_timestamp                   as activity_ts,
        date_trunc('month', email_timestamp)::date as activity_month,
        owner_id                          as owner_id,

        case
            when contacts_json::text ~ '\d+'
                then ((regexp_match(contacts_json::text, '(\d+)'))[1])::bigint
        end                               as contact_id,

        case
            when companies_json::text ~ '\d+'
                then ((regexp_match(companies_json::text, '(\d+)'))[1])::bigint
        end                               as company_id,

        null::bigint                      as deal_id,
        created_at_raw                    as created_at,
        updated_at_raw                    as updated_at,
        extracted_at
    from "warehouse"."marts_staging"."stg_engagement_emails"
    where email_timestamp is not null
),

meetings as (
    select
        m.meeting_id                      as activity_id,
        'meeting'                         as activity_type,
        m.meeting_timestamp               as activity_ts,
        date_trunc('month', m.meeting_timestamp)::date as activity_month,
        m.owner_id                        as owner_id,
        mc.contact_id                     as contact_id,
        mco.company_id                    as company_id,
        null::bigint                      as deal_id,
        m.created_at_raw                  as created_at,
        m.updated_at_raw                  as updated_at,
        m.extracted_at
    from "warehouse"."marts_staging"."stg_engagements_meetings" m
    left join "warehouse"."marts_staging"."stg_meeting_contacts"  mc
           on mc.meeting_id = m.meeting_id
    left join "warehouse"."marts_staging"."stg_meeting_companies" mco
           on mco.meeting_id = m.meeting_id
    where m.meeting_timestamp is not null
),

tasks as (
    select
        task_id                           as activity_id,
        'task'                            as activity_type,
        task_timestamp                    as activity_ts,
        date_trunc('month', task_timestamp)::date as activity_month,
        owner_id                          as owner_id,

        case
            when contacts_json::text ~ '\d+'
                then ((regexp_match(contacts_json::text, '(\d+)'))[1])::bigint
        end                               as contact_id,

        case
            when companies_json::text ~ '\d+'
                then ((regexp_match(companies_json::text, '(\d+)'))[1])::bigint
        end                               as company_id,

        null::bigint                      as deal_id,
        created_at_raw                    as created_at,
        updated_at_raw                    as updated_at,
        extracted_at
    from "warehouse"."marts_staging"."stg_engagements_tasks"
    where task_timestamp is not null
)

select
    activity_id,
    activity_type,
    activity_ts,
    activity_month,
    owner_id,
    contact_id,
    company_id,
    deal_id,
    created_at,
    updated_at,
    extracted_at
from calls

union all

select
    activity_id,
    activity_type,
    activity_ts,
    activity_month,
    owner_id,
    contact_id,
    company_id,
    deal_id,
    created_at,
    updated_at,
    extracted_at
from emails

union all

select
    activity_id,
    activity_type,
    activity_ts,
    activity_month,
    owner_id,
    contact_id,
    company_id,
    deal_id,
    created_at,
    updated_at,
    extracted_at
from meetings

union all

select
    activity_id,
    activity_type,
    activity_ts,
    activity_month,
    owner_id,
    contact_id,
    company_id,
    deal_id,
    created_at,
    updated_at,
    extracted_at
from tasks