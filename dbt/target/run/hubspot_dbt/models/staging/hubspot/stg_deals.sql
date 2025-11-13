
  create view "warehouse"."marts_staging"."stg_deals__dbt_tmp"
    
    
  as (
    -- models/staging/hubspot/stg_deals.sql

with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at
    from warehouse.airbyte_internal.raw_hubspot_raw__stream_deals
),

base as (
    select
        -- Identifiants & métadonnées
        (j->>'id')::bigint                              as deal_id,
        (j->>'createdAt')::timestamptz                  as created_at_raw,
        (j->>'updatedAt')::timestamptz                  as updated_at_raw,
        _airbyte_extracted_at                           as extracted_at,

        -- Propriétés principales HubSpot
        (j->'properties'->>'dealname')                  as deal_name,
        (j->'properties'->>'pipeline')                  as pipeline_id,
        (j->'properties'->>'dealstage')                 as deal_stage_id,
        nullif(j->'properties'->>'dealtype','')         as deal_type,
        (j->'properties'->>'deal_currency_code')        as currency,

        -- Ownership / relations
        nullif(j->'properties'->>'hubspot_owner_id','')::bigint
                                                        as owner_id,
        nullif(j->'properties'->>'hs_primary_associated_company','')::bigint
                                                        as primary_company_id,

        -- Montants
        nullif(j->'properties'->>'hs_acv','')::numeric  as hs_acv,
        nullif(j->'properties'->>'hs_tcv','')::numeric  as hs_tcv,
        nullif(j->'properties'->>'amount_in_home_currency','')::numeric
                                                        as amount_in_home_currency,
        nullif(j->'properties'->>'hs_closed_amount','')::numeric
                                                        as hs_closed_amount,
        nullif(j->'properties'->>'hs_closed_amount_in_home_currency','')::numeric
                                                        as hs_closed_amount_in_home_currency,

        -- États du deal
        coalesce((j->'properties'->>'hs_is_closed')::boolean, false)
                                                        as is_closed,
        coalesce((j->'properties'->>'hs_is_closed_won')::boolean, false)
                                                        as is_won,
        coalesce((j->'properties'->>'hs_is_closed_lost')::boolean, false)
                                                        as is_lost,

        -- Probabilités & analytics
        nullif(j->'properties'->>'hs_deal_stage_probability','')::numeric
                                                        as stage_probability,
        nullif(j->'properties'->>'hs_deal_stage_probability_shadow','')::numeric
                                                        as stage_probability_shadow,
        (j->'properties'->>'hs_analytics_latest_source')
                                                        as analytics_latest_source,
        nullif(j->'properties'->>'hs_analytics_latest_source_timestamp','')::timestamptz
                                                        as analytics_latest_source_ts,
        nullif(j->'properties'->>'hs_analytics_source_data_1','')
                                                        as analytics_source_data_1,

        -- Dates clés métier
        nullif(j->'properties'->>'createdate','')::timestamptz
                                                        as createdate,
        nullif(j->'properties'->>'closedate','')::timestamptz
                                                        as closedate,
        nullif(j->'properties'->>'hs_lastmodifieddate','')::timestamptz
                                                        as lastmodifieddate,
        nullif(j->'properties'->>'hubspot_owner_assigneddate','')::timestamptz
                                                        as owner_assigned_at,

        -- Aide pour les ponts (associations)
        j->'companies'                                   as companies_json,
        j->'line_items'                                  as line_items_json
    from src
)

select *
from base
  );