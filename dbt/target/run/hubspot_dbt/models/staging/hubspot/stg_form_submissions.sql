
  create view "warehouse"."marts_staging"."stg_form_submissions__dbt_tmp"
    
    
  as (
    with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from warehouse.airbyte_internal.raw_hubspot_raw__stream_form_submissions_airbyte_tmp
),

base as (
    select
        (j->>'formId')                       as form_id,
        (j->>'pageUrl')                      as page_url,
        (j->>'conversionId')                 as conversion_id,

        -- timestamps en millisecondes
        case when j->>'submittedAt' is not null
             then to_timestamp((j->>'submittedAt')::bigint / 1000.0)
             end                             as submitted_at,
        case when j->>'updatedAt' is not null
             then to_timestamp((j->>'updatedAt')::bigint / 1000.0)
             end                             as updated_at,

        j->'values'                          as values_json,
        extracted_at
    from src
),

extracted_fields as (
    select
        form_id,
        page_url,
        conversion_id,
        submitted_at,
        updated_at,
        extracted_at,
        values_json,

        -- extraction pratique des champs standard (email, firstname, lastname)
        (
          select v->>'value'
          from jsonb_array_elements(values_json) as v
          where v->>'name' = 'email'
          limit 1
        ) as email,

        (
          select v->>'value'
          from jsonb_array_elements(values_json) as v
          where v->>'name' = 'firstname'
          limit 1
        ) as first_name,

        (
          select v->>'value'
          from jsonb_array_elements(values_json) as v
          where v->>'name' = 'lastname'
          limit 1
        ) as last_name
    from base
)

select *
from extracted_fields
  );