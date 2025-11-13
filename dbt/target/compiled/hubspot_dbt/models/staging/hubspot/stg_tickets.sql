with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from warehouse.airbyte_internal.raw_hubspot_raw__stream_tickets
),

base as (
    select
        (j->>'id')::bigint                           as ticket_id,

        nullif(j->>'createdAt','')::timestamptz     as created_at_raw,
        nullif(j->>'updatedAt','')::timestamptz     as updated_at_raw,

        -- propriétés
        (j->'properties'->>'subject')               as subject,
        (j->'properties'->>'content')               as content,
        (j->'properties'->>'source_type')           as source_type,

        nullif(j->'properties'->>'createdate','')::timestamptz      as createdate,
        nullif(j->'properties'->>'hs_lastmodifieddate','')::timestamptz as lastmodifieddate,
        nullif(j->'properties'->>'closed_date','')::timestamptz     as closed_date,

        (j->'properties'->>'hs_pipeline')           as pipeline_id,
        (j->'properties'->>'hs_pipeline_stage')     as pipeline_stage_id,

        nullif(j->'properties'->>'time_to_close','')::bigint        as time_to_close_ms,

        nullif(j->'properties'->>'created_by','')::bigint           as created_by_contact_id,
        nullif(j->'properties'->>'hs_primary_company_id','')::bigint as primary_company_id,
        (j->'properties'->>'hs_primary_company_name')               as primary_company_name,

        nullif(j->'properties'->>'hs_last_closed_date','')::timestamptz as last_closed_date,

        -- flags dérivables
        (nullif(j->'properties'->>'closed_date','') is not null)    as is_closed,

        -- associations racine
        j->'contacts'   as contacts_json,
        j->'companies'  as companies_json,

        extracted_at
    from src
)

select
    ticket_id,
    coalesce(createdate, created_at_raw)       as created_at,
    coalesce(lastmodifieddate, updated_at_raw) as updated_at,
    closed_date,
    is_closed,

    subject,
    content,
    source_type,

    pipeline_id,
    pipeline_stage_id,
    time_to_close_ms,

    created_by_contact_id,
    primary_company_id,
    primary_company_name,

    last_closed_date,
    contacts_json,
    companies_json,
    extracted_at
from base