

with src as (
    select
        *
    from "warehouse"."marts_staging"."stg_goals"
),

/* ===============================
   1. DÉDUPLICATION DES GOALS
   =============================== */

dedup as (
    select
        src.*,
        row_number() over (
            partition by goal_id
            order by
                cast(updated_at_raw as timestamp) desc,
                cast(extracted_at as timestamp) desc
        ) as rn
    from src
),

latest as (
    select *
    from dedup
    where rn = 1
),

/* ===============================
   2. NORMALISATION
   =============================== */

normalized as (
    select
        goal_id::bigint                 as goal_id,
        goal_name,
        goal_type,      -- ex: 'sales_quota'
        status,         -- ex: 'in_progress', 'complete'
        outcome,        -- ex: 'in_progress', 'success', etc.

        cast(start_datetime as timestamp) as start_at,
        cast(end_datetime as timestamp)   as end_at,

        owner_id::bigint               as owner_id,
        assignee_user_id::bigint       as assignee_user_id,

        target_amount::numeric                      as target_amount,
        target_amount_home_currency::numeric        as target_amount_home_currency,
        kpi_value::numeric                          as kpi_value,
        kpi_progress_percent::numeric               as kpi_progress_percent,
        currency_code,

        cast(created_at_raw as timestamp)  as created_at_raw,
        cast(updated_at_raw as timestamp)  as updated_at_raw,
        cast(extracted_at as timestamp)    as extracted_at
    from latest
),

/* ===============================
   3. ENRICHISSEMENT AVEC OWNER
   =============================== */

with_owner as (
    select
        g.*,
        o.email      as owner_email,
        o.first_name as owner_first_name,
        o.last_name  as owner_last_name,
        o.is_archived as owner_is_archived
    from normalized g
    left join "warehouse"."marts_intermediate"."int_owners" o
        on g.owner_id = o.owner_id
),

/* ===============================
   4. DÉRIVÉS POUR FACILITER LES KPI
   =============================== */

final as (
    select
        *,
        -- année / mois de début (utile en group by)
        extract(year  from start_at)::int as start_year,
        extract(month from start_at)::int as start_month,

        extract(year  from end_at)::int   as end_year,
        extract(month from end_at)::int   as end_month
    from with_owner
)

select *
from final