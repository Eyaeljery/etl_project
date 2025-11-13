with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from {{ source('hubspot', 'raw_hubspot_raw__stream_engagements_emails') }}
)

select
    (j->>'id')::bigint                                   as email_id,

    nullif(j->>'createdAt','')::timestamptz             as created_at_raw,
    nullif(j->>'updatedAt','')::timestamptz             as updated_at_raw,

    nullif(j->'properties'->>'hs_timestamp','')::timestamptz as email_timestamp,
    (j->'properties'->>'hs_email_subject')              as subject,
    (j->'properties'->>'hs_email_status')               as status,
    (j->'properties'->>'hs_email_direction')            as direction, -- INCOMING / OUTGOING

    nullif(j->'properties'->>'hubspot_owner_id','')::bigint as owner_id,

    j->'contacts'   as contacts_json,
    j->'companies'  as companies_json,
    j->'deals'      as deals_json,
    j->'tickets'    as tickets_json,

    extracted_at
from src
