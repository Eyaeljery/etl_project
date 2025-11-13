with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from warehouse.airbyte_internal.raw_hubspot_raw__stream_companies
)

select
    (j->>'id')::bigint                                   as company_id,

    -- dates racine
    nullif(j->>'createdAt','')::timestamptz             as created_at_raw,
    nullif(j->>'updatedAt','')::timestamptz             as updated_at_raw,

    -- propriétés
    (j->'properties'->>'name')                          as name,
    nullif(j->'properties'->>'domain','')               as domain,
    nullif(j->'properties'->>'country','')              as country,
    nullif(j->'properties'->>'industry','')             as industry,

    -- dates hubspot
    nullif(j->'properties'->>'createdate','')::timestamptz          as createdate,
    nullif(j->'properties'->>'hs_lastmodifieddate','')::timestamptz as lastmodifieddate,

    extracted_at
from src