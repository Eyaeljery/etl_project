
  create view "warehouse"."marts_intermediate"."int_owners__dbt_tmp"
    
    
  as (
    

with owners_all as (
    select
        owner_id,
        email,
        first_name,
        last_name,
        -- stg_owners_all a une colonne is_archived
        is_archived as archived_flag,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        user_id,
        user_id_incl_inactive,
        cast(extracted_at as timestamp) as extracted_at
    from "warehouse"."marts_staging"."stg_owners_all"
),

owners_archived as (
    select
        owner_id,
        email,
        first_name,
        last_name,
        archived_flag,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        -- pas de user_id dans cette table => null
        cast(null as bigint) as user_id,
        user_id_incl_inactive,
        cast(extracted_at as timestamp) as extracted_at
    from "warehouse"."marts_staging"."stg_owners_archived"
),

unioned as (
    select * from owners_all
    union all
    select * from owners_archived
),

-- on garde la dernière version de chaque owner
dedup as (
    select
        unioned.*,
        row_number() over (
            partition by owner_id
            order by updated_at desc, extracted_at desc
        ) as rn
    from unioned
),

latest as (
    select *
    from dedup
    where rn = 1
)

select
    owner_id,
    email,
    first_name,
    last_name,
    coalesce(archived_flag, false) as is_archived,
    created_at,
    updated_at,
    -- on normalise sur un seul identifiant "user_id"
    coalesce(user_id, user_id_incl_inactive) as user_id,
    extracted_at
from latest
  );