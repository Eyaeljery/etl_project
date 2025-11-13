with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from {{ source('hubspot', 'raw_hubspot_raw__stream_engagements_meetings') }}
)

select
    (j->>'id')::bigint                                   as meeting_id,

    nullif(j->>'createdAt','')::timestamptz             as created_at_raw,
    nullif(j->>'updatedAt','')::timestamptz             as updated_at_raw,

    nullif(j->'properties'->>'hs_timestamp','')::timestamptz       as meeting_timestamp,
    nullif(j->'properties'->>'hs_meeting_start_time','')::timestamptz as start_time,
    nullif(j->'properties'->>'hs_meeting_end_time','')::timestamptz   as end_time,

    (j->'properties'->>'hs_meeting_title')              as title,
    (j->'properties'->>'hs_meeting_body')               as body,

    nullif(j->'properties'->>'hubspot_owner_id','')::bigint as owner_id,

    -- associations
    j->'contacts'   as contacts_json,
    j->'companies'  as companies_json,
    j->'deals'      as deals_json,
    j->'tickets'    as tickets_json,

    extracted_at
from src
