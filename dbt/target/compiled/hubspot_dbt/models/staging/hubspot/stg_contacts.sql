with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from warehouse.airbyte_internal.raw_hubspot_raw__stream_contacts
),

base as (
    select
        -- Identifiant hubspot du contact
        (j->>'id')::bigint                                   as contact_id,

        -- Timestamps bruts au niveau racine
        nullif(j->>'createdAt','')::timestamptz             as created_at_raw,
        nullif(j->>'updatedAt','')::timestamptz             as updated_at_raw,

        -- Propriétés "classiques"
        nullif(j->'properties'->>'email','')                as email,
        nullif(j->'properties'->>'firstname','')            as first_name,
        nullif(j->'properties'->>'lastname','')             as last_name,
        nullif(j->'properties'->>'country','')              as country,

        -- Funnel / marketing
        nullif(j->'properties'->>'lifecyclestage','')       as lifecycle_stage,      -- lead, mql, sql, opportunity…
        nullif(j->'properties'->>'hs_lead_status','')       as lead_status,          -- Nouveau, À contacter, etc.
        nullif(j->'properties'->>'hs_marketable_status','') as marketable_status,

        -- Ownership (pour rattacher au commercial)
        nullif(j->'properties'->>'hubspot_owner_id','')::bigint as owner_id,

        -- Dates importantes pour les KPIs
        nullif(j->'properties'->>'createdate','')::timestamptz              as createdate,
        nullif(j->'properties'->>'hs_lastmodifieddate','')::timestamptz     as lastmodifieddate,
        nullif(j->'properties'->>'hs_lifecyclestage_lead_date','')::timestamptz as lifecyclestage_lead_date,
        nullif(j->'properties'->>'hs_lifecyclestage_opportunity_date','')::timestamptz as lifecyclestage_opportunity_date,
        nullif(j->'properties'->>'hs_lifecyclestage_customer_date','')::timestamptz   as lifecyclestage_customer_date,

        -- Analytics HubSpot (pour conversion visite → lead)
        nullif(j->'properties'->>'hs_analytics_source','')                   as analytics_source,
        nullif(j->'properties'->>'hs_analytics_first_url','')               as analytics_first_url,
        nullif(j->'properties'->>'hs_analytics_first_referrer','')          as analytics_first_referrer,
        nullif(j->'properties'->>'hs_analytics_num_page_views','')::int     as num_page_views,
        nullif(j->'properties'->>'hs_analytics_num_visits','')::int         as num_visits,
        nullif(j->'properties'->>'hs_analytics_first_timestamp','')::timestamptz as first_analytics_ts,
        nullif(j->'properties'->>'hs_analytics_last_timestamp','')::timestamptz  as last_analytics_ts,
        nullif(j->'properties'->>'hs_analytics_first_touch_converting_campaign','') as first_touch_campaign,
        nullif(j->'properties'->>'hs_analytics_last_touch_converting_campaign','')  as last_touch_campaign,

        -- Liste de companies associées (souvent un array d’ids)
        j->'companies'                                      as companies_json,

        -- Colonne technique Airbyte
        extracted_at
    from src
)

select
    contact_id,

    -- on priorise les dates 'properties', sinon on fallback sur les dates racine
    coalesce(createdate, created_at_raw)       as created_at,
    coalesce(lastmodifieddate, updated_at_raw) as updated_at,

    email,
    first_name,
    last_name,
    country,

    lifecycle_stage,
    lead_status,
    marketable_status,
    owner_id,

    lifecyclestage_lead_date,
    lifecyclestage_opportunity_date,
    lifecyclestage_customer_date,

    analytics_source,
    analytics_first_url,
    analytics_first_referrer,
    num_page_views,
    num_visits,
    first_analytics_ts,
    last_analytics_ts,
    first_touch_campaign,
    last_touch_campaign,

    companies_json,
    extracted_at
from base