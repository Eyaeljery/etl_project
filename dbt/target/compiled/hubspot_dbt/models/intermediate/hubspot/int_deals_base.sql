

with deals_src as (
    select
        *
    from "warehouse"."marts_staging"."stg_deals"
),

/* ===============================
   1. DÉDUPLICATION DES DEALS
   =============================== */

deals_dedup as (
    select
        deals_src.*,
        row_number() over (
            partition by deal_id
            order by
                cast(updated_at_raw as timestamp) desc,
                cast(extracted_at as timestamp) desc
        ) as rn
    from deals_src
),

latest as (
    select *
    from deals_dedup
    where rn = 1
),

/* ===============================
   2. NORMALISATION DES CHAMPS
   =============================== */

deals_base as (
    select
        deal_id                         as deal_pk,
        deal_id                         as deal_id,
        deal_name,
        pipeline_id,
        deal_stage_id,

        deal_type,
        currency,

        owner_id,
        primary_company_id,

        -- montants
        hs_acv,
        hs_tcv,
        amount_in_home_currency,
        hs_closed_amount,
        hs_closed_amount_in_home_currency,

        -- statut du deal
        is_closed::boolean              as is_closed,
        is_won::boolean                 as is_won,
        is_lost::boolean                as is_lost,

        stage_probability,
        stage_probability_shadow,

        -- analytics / source
        analytics_latest_source,
        analytics_latest_source_ts,
        analytics_source_data_1,

        -- dates principales
        cast(createdate as timestamp)   as created_at,
        cast(closedate as timestamp)    as closed_at,
        cast(lastmodifieddate as timestamp) as last_modified_at,
        cast(owner_assigned_at as timestamp) as owner_assigned_at,

        -- dates dérivées
        cast(createdate as date)        as created_date,
        cast(closedate as date)         as closed_date,

        -- métadonnées de chargement
        cast(created_at_raw as timestamp)  as created_at_raw,
        cast(updated_at_raw as timestamp)  as updated_at_raw,
        cast(extracted_at as timestamp)    as extracted_at,

        -- associations brutes
        companies_json,
        line_items_json
    from latest
),

/* ===============================
   3. ENRICHISSEMENT PIPELINE
   =============================== */

pipeline as (
    select
        pipeline_id,
        pipeline_label,
        stage_id,
        stage_label,
        is_closed_stage::boolean   as is_closed_stage,
        stage_probability          as pipeline_stage_probability,
        display_order
    from "warehouse"."marts_staging"."stg_deal_pipeline"
),

deals_with_pipeline as (
    select
        d.*,
        p.pipeline_label,
        p.stage_label,
        p.is_closed_stage,
        p.display_order        as stage_display_order,
        p.pipeline_stage_probability
    from deals_base d
    left join pipeline p
        on d.pipeline_id = p.pipeline_id
       and d.deal_stage_id = p.stage_id
),

/* ===============================
   4. ENRICHISSEMENT OWNER
   =============================== */

deals_enriched as (
    select
        d.*,
        o.email      as owner_email,
        o.first_name as owner_first_name,
        o.last_name  as owner_last_name,
        o.is_archived as owner_is_archived
    from deals_with_pipeline d
    left join "warehouse"."marts_intermediate"."int_owners" o
        on d.owner_id = o.owner_id
),

/* ===============================
   5. DÉRIVÉS POUR LES KPIs
   =============================== */

final as (
    select
        *,
        -- catégorie de statut pour simplifier les analyses
        case
            when is_won = true then 'WON'
            when is_lost = true then 'LOST'
            when is_closed = true and is_won is not true and is_lost is not true then 'CLOSED_OTHER'
            else 'OPEN'
        end as deal_status,

        -- durée du cycle de vente en jours (pour les deals fermés)
        case
            when closed_at is not null
                then extract(epoch from (closed_at - created_at)) / 86400.0
            else null
        end as sales_cycle_days
    from deals_enriched
)

select *
from final