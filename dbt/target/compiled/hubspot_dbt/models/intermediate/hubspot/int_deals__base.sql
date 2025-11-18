

with ranked as (
    select
        sd.*,
        row_number() over (
            partition by deal_id
            order by extracted_at desc
        ) as rn
    from "warehouse"."marts_staging"."stg_deals" sd
),

dedup as (
    select *
    from ranked
    where rn = 1
),

normalized as (
    select
        deal_id,
        deal_name,
        pipeline_id,
        deal_stage_id,
        owner_id,
        primary_company_id,

        -- dates : on reconstruit un created_at “propre”
        coalesce(createdate, created_at_raw) as created_datetime,
        closedate                            as closed_datetime,

        date_trunc('month', coalesce(createdate, created_at_raw)) as created_month,
        date_trunc('month', closedate)                            as closed_month,
        date_trunc('year',  closedate)                            as closed_year,

        -- flags
        coalesce(is_won,   false) as is_won,
        coalesce(is_lost,  false) as is_lost,
        coalesce(is_closed,false) as is_closed,

        -- devise & CA
        currency,

        -- revenue en monnaie “home” (logique centrale pour tous les KPIs CA)
        coalesce(
          hs_closed_amount_in_home_currency,
          amount_in_home_currency,
          hs_closed_amount,
          hs_tcv,
          hs_acv,
          0
        )::numeric as revenue_home,

        extracted_at
    from dedup
)

select *
from normalized