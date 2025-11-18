
  create view "warehouse"."marts_intermediate"."int_goals__by_month__dbt_tmp"
    
    
  as (
    -- models/intermediate/hubspot/int_goals__by_month.sql



with base as (
    select
        goal_id,
        goal_name,
        goal_type,
        -- on suppose que stg_goals expose déjà owner_id (cast de hubspot_owner_id)
        owner_id,
        start_datetime,
        end_datetime,
        target_amount_home
    from "warehouse"."marts_staging"."stg_goals"
    where goal_type = 'sales_quota'
      and start_datetime is not null
      and end_datetime   is not null
      and target_amount_home is not null
),

expanded as (
    select
        goal_id,
        goal_name,
        goal_type,
        owner_id,
        date_trunc('month', start_datetime)::date as goal_start_month,
        date_trunc('month', end_datetime)::date   as goal_end_month,
        target_amount_home
    from base
),

month_series as (
    select
        e.*,
        generate_series(
            e.goal_start_month,
            e.goal_end_month,
            interval '1 month'
        )::date as month_start
    from expanded e
),

with_counts as (
    select
        *,
        count(*) over (partition by goal_id) as nb_months
    from month_series
)

select
    goal_id,
    goal_name,
    goal_type,
    owner_id,
    month_start                                       as goal_month,
    (target_amount_home / nullif(nb_months,0))::numeric
                                                      as monthly_target_amount
from with_counts
  );