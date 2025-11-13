with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from {{ source('hubspot', 'raw_hubspot_raw__stream_engagements_tasks') }}
)

select
    (j->>'id')::bigint                                   as task_id,

    nullif(j->>'createdAt','')::timestamptz             as created_at_raw,
    nullif(j->>'updatedAt','')::timestamptz             as updated_at_raw,

    (j->'properties'->>'hs_task_subject')               as subject,
    (j->'properties'->>'hs_task_type')                  as task_type,
    (j->'properties'->>'hs_task_family')                as task_family,
    (j->'properties'->>'hs_task_status')                as status,
    (j->'properties'->>'hs_task_priority')              as priority,

    coalesce((j->'properties'->>'hs_task_is_completed')::int,0)      as is_completed,
    coalesce((j->'properties'->>'hs_task_is_overdue')::boolean,false) as is_overdue,
    coalesce((j->'properties'->>'hs_task_missed_due_date')::boolean,false) as is_missed_due_date,

    nullif(j->'properties'->>'hs_timestamp','')::timestamptz         as task_timestamp,
    nullif(j->'properties'->>'hs_task_last_sales_activity_timestamp','')::timestamptz as last_sales_activity_ts,

    nullif(j->'properties'->>'hubspot_owner_id','')::bigint          as owner_id,

    j->'contacts'   as contacts_json,
    j->'companies'  as companies_json,
    j->'deals'      as deals_json,
    j->'tickets'    as tickets_json,

    extracted_at
from src
