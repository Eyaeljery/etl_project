

with base as (
    select
        owner_id,
        total_post_sale_activities,
        calls_post_sale_count,
        emails_post_sale_count,
        tasks_post_sale_count,
        meetings_post_sale_count,
        notes_post_sale_count,
        tasks_post_sale_total,
        tasks_post_sale_completed
    from "warehouse"."marts_marts"."mart_activity_post_sale"
),

agg as (
    select
        owner_id,
        sum(coalesce(total_post_sale_activities, 0)) as total_post_sale_activities,
        sum(coalesce(calls_post_sale_count, 0))       as calls_post_sale_count,
        sum(coalesce(emails_post_sale_count, 0))      as emails_post_sale_count,
        sum(coalesce(tasks_post_sale_count, 0))       as tasks_post_sale_count,
        sum(coalesce(meetings_post_sale_count, 0))    as meetings_post_sale_count,
        sum(coalesce(notes_post_sale_count, 0))       as notes_post_sale_count,
        sum(coalesce(tasks_post_sale_total, 0))       as tasks_post_sale_total,
        sum(coalesce(tasks_post_sale_completed, 0))   as tasks_post_sale_completed
    from base
    group by owner_id
),

owners as (
    select
        owner_id,
        email       as owner_email,
        first_name  as owner_first_name,
        last_name   as owner_last_name
    from "warehouse"."marts_intermediate"."int_owners"
),

final as (
    select
        a.owner_id,
        o.owner_email,
        o.owner_first_name,
        o.owner_last_name,
        a.total_post_sale_activities,
        a.calls_post_sale_count,
        a.emails_post_sale_count,
        a.tasks_post_sale_count,
        a.meetings_post_sale_count,
        a.notes_post_sale_count,
        a.tasks_post_sale_total,
        a.tasks_post_sale_completed,
        case
            when a.tasks_post_sale_total > 0
            then a.tasks_post_sale_completed::float / a.tasks_post_sale_total::float
            else null
        end as tasks_post_sale_completion_ratio
    from agg a
    left join owners o
        on a.owner_id = o.owner_id
)

select *
from final