
  create view "warehouse"."marts_intermediate"."int_contacts__last_activity__dbt_tmp"
    
    
  as (
    -- depends_on: "warehouse"."marts_staging"."stg_contacts"
-- depends_on: "warehouse"."marts_intermediate"."int_engagements__activity"




with contacts as (
    select
        contact_id,
        created_at        as contact_created_at,
        lifecycle_stage,
        owner_id
    from "warehouse"."marts_staging"."stg_contacts"
),

last_activity as (
    select
        contact_id,
        max(activity_ts) as last_activity_at
    from "warehouse"."marts_intermediate"."int_engagements__activity"
    where contact_id is not null
    group by contact_id
)

select
    c.contact_id,
    c.contact_created_at,
    c.lifecycle_stage,
    c.owner_id,
    a.last_activity_at
from contacts c
left join last_activity a
    on c.contact_id = a.contact_id
  );