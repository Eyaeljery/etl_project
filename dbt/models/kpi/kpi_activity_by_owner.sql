{{ config(
    materialized = 'view'
) }}

with engagements as (
    select
        owner_id,
        engagement_type,
        activity_timestamp
    from {{ ref('int_engagements_all') }}
),

owners as (
    select
        owner_id,
        email       as owner_email,
        first_name,
        last_name,
        (coalesce(first_name, '') ||
         case
             when last_name is not null and last_name <> ''
                 then ' ' || last_name
             else ''
         end
        ) as owner_full_name
    from {{ ref('int_owners') }}
),

agg as (
    select
        owner_id,
        count(*) as total_engagements,
        count(*) filter (where engagement_type = 'CALL')    as calls_count,
        count(*) filter (where engagement_type = 'EMAIL')   as emails_count,
        count(*) filter (where engagement_type = 'TASK')    as tasks_count,
        count(*) filter (where engagement_type = 'MEETING') as meetings_count,
        count(*) filter (where engagement_type = 'NOTE')    as notes_count
    from engagements
    group by owner_id
),

final as (
    select
        o.owner_id,
        o.owner_full_name,
        o.owner_email,
        coalesce(a.total_engagements, 0) as total_engagements,
        coalesce(a.calls_count, 0)       as calls_count,
        coalesce(a.emails_count, 0)      as emails_count,
        coalesce(a.tasks_count, 0)       as tasks_count,
        coalesce(a.meetings_count, 0)    as meetings_count,
        coalesce(a.notes_count, 0)       as notes_count
    from owners o
    left join agg a
        on o.owner_id = a.owner_id
)

select *
from final
order by total_engagements desc