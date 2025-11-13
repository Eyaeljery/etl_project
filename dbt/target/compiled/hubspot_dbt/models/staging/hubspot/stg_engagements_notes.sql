with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from warehouse.airbyte_internal.raw_hubspot_raw__stream_engagements_notes
)

select
    (j->>'id')::bigint                                   as note_id,

    nullif(j->>'createdAt','')::timestamptz             as created_at_raw,
    nullif(j->>'updatedAt','')::timestamptz             as updated_at_raw,

    (j->'properties'->>'hs_note_body')                  as body_html,
    (j->'properties'->>'hs_body_preview')               as body_text,

    nullif(j->'properties'->>'hubspot_owner_id','')::bigint as owner_id,

    -- associations
    j->'contacts'   as contacts_json,
    j->'companies'  as companies_json,
    j->'deals'      as deals_json,
    j->'tickets'    as tickets_json,

    extracted_at
from src