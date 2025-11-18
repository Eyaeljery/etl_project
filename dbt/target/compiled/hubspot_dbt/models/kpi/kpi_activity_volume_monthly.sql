

with engagements as (
    select
        date_trunc('month', activity_timestamp) as month_start,
        engagement_type
    from "warehouse"."marts_intermediate"."int_engagements_all"
    where activity_timestamp is not null
),


monthly as (
    select
        month_start,
        count(*) as total_engagements,
        count(*) filter (where engagement_type = 'CALL')    as calls_count,
        count(*) filter (where engagement_type = 'EMAIL')   as emails_count,
        count(*) filter (where engagement_type = 'TASK')    as tasks_created_count,
        count(*) filter (where engagement_type = 'MEETING') as meetings_count,
        count(*) filter (where engagement_type = 'NOTE')    as notes_count
    from engagements
    group by month_start
),


final as (
    select *
    from monthly
    -- 🔥 FILTRE POUR NE PAS AFFICHER LES MOIS FUTURS
    where month_start <= date_trunc('month', current_date)
)

select *
from final