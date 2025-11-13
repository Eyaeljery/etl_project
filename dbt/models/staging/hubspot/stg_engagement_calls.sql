with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from {{ source('hubspot', 'raw_hubspot_raw__stream_engagements_calls') }}
)

select
    (j->>'id')::bigint                                   as call_id,

    nullif(j->>'createdAt','')::timestamptz             as created_at_raw,
    nullif(j->>'updatedAt','')::timestamptz             as updated_at_raw,

    nullif(j->'properties'->>'hs_timestamp','')::timestamptz as call_timestamp,
    nullif(j->'properties'->>'hs_call_duration','')::int     as duration_seconds,
    (j->'properties'->>'hs_call_direction')             as direction, -- INBOUND / OUTBOUND
    (j->'properties'->>'hs_call_status')                as status,
    (j->'properties'->>'hs_call_outcome')               as outcome,

    nullif(j->'properties'->>'hubspot_owner_id','')::bigint as owner_id,

    j->'contacts'   as contacts_json,
    j->'companies'  as companies_json,
    j->'deals'      as deals_json,
    j->'tickets'    as tickets_json,

    extracted_at
from src
