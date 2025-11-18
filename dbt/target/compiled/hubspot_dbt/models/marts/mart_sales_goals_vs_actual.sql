

with goals as (
    select
        goal_id,
        goal_name,
        goal_type,
        status,
        outcome,

        start_at,
        end_at,
        start_year,
        start_month,
        end_year,
        end_month,

        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        owner_is_archived,

        target_amount_home_currency,
        kpi_value,
        kpi_progress_percent,
        currency_code,

        created_at_raw,
        updated_at_raw,
        extracted_at
    from "warehouse"."marts_intermediate"."int_goals"
    -- si tu veux filtrer : par ex. uniquement les objectifs de CA
    -- where goal_type = 'REVENUE'
),

won_deals as (
    select
        deal_id,
        owner_id,
        owner_email,
        hs_closed_amount_in_home_currency,
        closed_at,
        closed_date,
        closed_month,
        deal_status
    from "warehouse"."marts_marts"."mart_sales_deals"
    where deal_status = 'WON'
),

/* ============================================
   1. JOINTURE OBJECTIF ↔ DEALS (même owner + période)
   ============================================ */

goals_with_deals as (
    select
        g.goal_id,

        g.goal_name,
        g.goal_type,
        g.status,
        g.outcome,

        g.start_at,
        g.end_at,
        g.start_year,
        g.start_month,
        g.end_year,
        g.end_month,

        g.owner_id,
        g.owner_email,
        g.owner_first_name,
        g.owner_last_name,
        g.owner_is_archived,

        g.target_amount_home_currency,
        g.kpi_value,
        g.kpi_progress_percent,
        g.currency_code,

        d.deal_id,
        d.hs_closed_amount_in_home_currency,
        d.closed_at,
        d.closed_date,
        d.closed_month
    from goals g
    left join won_deals d
        on d.owner_id = g.owner_id
       and d.closed_at >= g.start_at
       and d.closed_at <  g.end_at
),

/* ============================================
   2. AGRÉGATION PAR OBJECTIF
   ============================================ */

agg as (
    select
        goal_id,

        max(goal_name)                        as goal_name,
        max(goal_type)                        as goal_type,
        max(status)                           as status,
        max(outcome)                          as outcome,

        max(start_at)                         as start_at,
        max(end_at)                           as end_at,
        max(start_year)                       as start_year,
        max(start_month)                      as start_month,
        max(end_year)                         as end_year,
        max(end_month)                        as end_month,

        max(owner_id)                         as owner_id,
        max(owner_email)                      as owner_email,
        max(owner_first_name)                 as owner_first_name,
        max(owner_last_name)                  as owner_last_name,
        bool_or(owner_is_archived)            as owner_is_archived,

        max(target_amount_home_currency)      as target_amount_home_currency,
        max(kpi_value)                        as kpi_value,
        max(kpi_progress_percent)             as kpi_progress_percent,
        max(currency_code)                    as currency_code,

        count(distinct deal_id)               as won_deals_count,
        coalesce(sum(hs_closed_amount_in_home_currency), 0) as won_amount_home_currency
    from goals_with_deals
    group by goal_id
),

/* ============================================
   3. CALCULS DÉRIVÉS : ATTEINTE % ET ÉCART
   ============================================ */

final as (
    select
        a.*,

        case
            when target_amount_home_currency is not null
                 and target_amount_home_currency <> 0
            then won_amount_home_currency / target_amount_home_currency
            else null
        end as attainment_ratio,  -- ex: 1.2 = 120% de l’objectif

        won_amount_home_currency - target_amount_home_currency
            as attainment_delta  -- écart en valeur absolue
    from agg a
)

select *
from final