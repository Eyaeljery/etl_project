
  create view "warehouse"."marts_marts"."mart_activity_owner_daily__dbt_tmp"
    
    
  as (
    

with engagements as (
    select
        activity_date::date           as activity_date,
        owner_id,
        owner_email,
        engagement_type,
        coalesce(is_completed, false) as is_completed
    from "warehouse"."marts_intermediate"."int_engagements_all"
),

agg as (
    select
        owner_id,
        owner_email,
        activity_date,

        count(*)                                           as total_activities,

        count(*) filter (where engagement_type = 'CALL')   as calls_count,
        count(*) filter (where engagement_type = 'EMAIL')  as emails_count,
        count(*) filter (where engagement_type = 'TASK')   as tasks_count,
        count(*) filter (where engagement_type = 'MEETING')as meetings_count,
        count(*) filter (where engagement_type = 'NOTE')   as notes_count,

        -- tâches complétées vs créées
        count(*) filter (
            where engagement_type = 'TASK'
        )                                                  as tasks_total,

        count(*) filter (
            where engagement_type = 'TASK'
              and is_completed = true
        )                                                  as tasks_completed
    from engagements
    group by owner_id, owner_email, activity_date
),

final as (
    select
        owner_id,
        owner_email,
        activity_date,

        total_activities,
        calls_count,
        emails_count,
        tasks_count,
        meetings_count,
        notes_count,
        tasks_total,
        tasks_completed,

        case
            when tasks_total > 0
            then tasks_completed::float / tasks_total
            else null
        end as tasks_completion_ratio
    from agg
)

select *
from final
  );