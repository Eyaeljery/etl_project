{{ config(
    materialized = 'view'
) }}

with deals as (
    select
        deal_pk,
        deal_id,
        deal_name,
        pipeline_id,
        pipeline_label,
        deal_stage_id,
        stage_label,
        is_closed_stage,
        stage_display_order,
        pipeline_stage_probability,

        deal_type,
        currency,

        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        owner_is_archived,

        primary_company_id,
        companies_json,

        hs_acv,
        hs_tcv,
        amount_in_home_currency,
        hs_closed_amount,
        hs_closed_amount_in_home_currency,

        is_closed,
        is_won,
        is_lost,
        deal_status,          -- déjà dérivé dans int_deals_base

        stage_probability,
        stage_probability_shadow,

        analytics_latest_source,
        analytics_latest_source_ts,
        analytics_source_data_1,

        created_at,
        created_date,
        closed_at,
        closed_date,
        last_modified_at,
        owner_assigned_at,

        sales_cycle_days,

        created_at_raw,
        updated_at_raw,
        extracted_at
    from {{ ref('int_deals_base') }}
),

final as (
    select
        -- clés
        deal_pk,
        deal_id,

        -- infos de base
        deal_name,
        deal_type,
        currency,

        -- owner
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        owner_is_archived,

        -- pipeline
        pipeline_id,
        pipeline_label,
        deal_stage_id,
        stage_label,
        is_closed_stage,
        stage_display_order,
        pipeline_stage_probability,

        -- status & flags
        is_closed,
        is_won,
        is_lost,
        deal_status,  -- OPEN / WON / LOST / CLOSED_OTHER

        -- montants
        hs_acv,
        hs_tcv,
        amount_in_home_currency,
        hs_closed_amount,
        hs_closed_amount_in_home_currency,

        -- dates brutes
        created_at,
        created_date,
        closed_at,
        closed_date,
        last_modified_at,
        owner_assigned_at,

        -- dérivés temporels pour les agrégations
        date_trunc('month', created_at) as created_month,
        date_trunc('month', closed_at)  as closed_month,

        extract(year  from created_at)::int as created_year,
        extract(month from created_at)::int as created_month_number,

        extract(year  from closed_at)::int  as closed_year,
        extract(month from closed_at)::int  as closed_month_number,

        -- cycle de vente
        sales_cycle_days,

        -- analytics source
        analytics_latest_source,
        analytics_latest_source_ts,
        analytics_source_data_1,

        -- association entreprise
        primary_company_id,
        companies_json,

        -- métadonnées
        created_at_raw,
        updated_at_raw,
        extracted_at
    from deals
)

select *
from final