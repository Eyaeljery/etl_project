

with tasks as (
    select
        date_trunc('month', activity_timestamp) as month_start,
        coalesce(is_completed, false)           as is_completed
    from "warehouse"."marts_intermediate"."int_engagements_all"
    where engagement_type = 'TASK'
      and activity_timestamp is not null
      and activity_timestamp < current_date      -- ✅ exclut les tâches futures
),



monthly as (
    select
        month_start,
        count(*) as tasks_total,
        count(*) filter (where is_completed = true) as tasks_completed
    from tasks
    group by month_start
),

final as (
    select
        month_start,
        tasks_total,
        tasks_completed,
        case
            when tasks_total > 0
            then tasks_completed::float / tasks_total::float
            else null
        end as tasks_completion_ratio
    from monthly
)

select *
from final