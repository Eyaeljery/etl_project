with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from {{ source('hubspot', 'raw_hubspot_raw__stream_goals') }}
)

select
    (j->>'id')::bigint                                   as goal_id,

    nullif(j->>'createdAt','')::timestamptz             as created_at_raw,
    nullif(j->>'updatedAt','')::timestamptz             as updated_at_raw,

    (j->'properties'->>'hs_goal_name')                  as goal_name,
    (j->'properties'->>'hs_goal_type')                  as goal_type,   -- sales_quota, etc.
    (j->'properties'->>'hs_status')                     as status,      -- in_progress, achieved, etc.
    (j->'properties'->>'hs_outcome')                    as outcome,

    nullif(j->'properties'->>'hs_start_datetime','')::timestamptz as start_datetime,
    nullif(j->'properties'->>'hs_end_datetime','')::timestamptz   as end_datetime,

    nullif(j->'properties'->>'hs_assignee_user_id','')::bigint    as assignee_user_id,
    nullif(j->'properties'->>'hubspot_owner_id','')::bigint       as owner_id,

    nullif(j->'properties'->>'hs_target_amount','')::numeric      as target_amount,
    nullif(j->'properties'->>'hs_target_amount_in_home_currency','')::numeric as target_amount_home_currency,

    nullif(j->'properties'->>'hs_kpi_value','')::numeric          as kpi_value,
    nullif(j->'properties'->>'hs_kpi_progress_percent','')::numeric as kpi_progress_percent,

    (j->'properties'->>'hs_goal_target_currency_code')            as currency_code,

    extracted_at
from src
