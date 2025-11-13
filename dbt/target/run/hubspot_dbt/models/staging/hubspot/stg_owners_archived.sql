
  create view "warehouse"."marts_staging"."stg_owners_archived__dbt_tmp"
    
    
  as (
    with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from warehouse.airbyte_internal.raw_hubspot_raw__stream_owners_archived
)

select
    (j->>'id')::bigint                               as owner_id,
    (j->>'email')                                    as email,
    (j->>'firstName')                               as first_name,
    (j->>'lastName')                                as last_name,
    (j->>'archived')::boolean                       as archived_flag,

    nullif(j->>'createdAt','')::timestamptz         as created_at,
    nullif(j->>'updatedAt','')::timestamptz         as updated_at,

    nullif(j->>'userIdIncludingInactive','')::bigint as user_id_incl_inactive,

    extracted_at
from src
  );