with active as (
    select
        owner_id::bigint                         as owner_id,
        email::text                              as email,
        first_name::text                         as first_name,
        last_name::text                          as last_name,
        false                                    as is_archived,
        created_at::timestamptz                  as created_at,
        updated_at::timestamptz                  as updated_at,
        user_id::bigint                          as user_id,
        user_id_incl_inactive::bigint            as user_id_incl_inactive,
        extracted_at
    from {{ ref('stg_owner') }}
),

archived as (
    select
        owner_id::bigint                         as owner_id,
        email::text                              as email,
        first_name::text                         as first_name,
        last_name::text                          as last_name,
        true                                     as is_archived,
        created_at::timestamptz                  as created_at,
        updated_at::timestamptz                  as updated_at,
        null::bigint                             as user_id,
        user_id_incl_inactive::bigint            as user_id_incl_inactive,
        extracted_at
    from {{ ref('stg_owners_archived') }}
)

select *
from active
union all
select *
from archived
