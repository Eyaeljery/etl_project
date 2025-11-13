with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at as extracted_at
    from warehouse.airbyte_internal.raw_hubspot_raw__stream_products
)

select
    (j->>'id')::bigint                               as product_id,

    nullif(j->>'createdAt','')::timestamptz         as created_at_raw,
    nullif(j->>'updatedAt','')::timestamptz         as updated_at_raw,

    (j->'properties'->>'name')                      as name,
    nullif(j->'properties'->>'hs_status','')        as status,
    nullif(j->'properties'->>'price','')::numeric   as price,
    nullif(j->'properties'->>'hs_price_eur','')::numeric as price_eur,

    nullif(j->'properties'->>'createdate','')::timestamptz          as createdate,
    nullif(j->'properties'->>'hs_lastmodifieddate','')::timestamptz as lastmodifieddate,

    extracted_at
from src