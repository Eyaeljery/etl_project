
  create view "warehouse"."marts_intermediate"."int_customers__base__dbt_tmp"
    
    
  as (
    

with customer_contacts as (
    -- Clients côté contacts : lifecycle_stage = 'customer'
    select
        contact_id,
        null::bigint      as company_id,
        'contact'         as customer_type,
        created_at        as first_seen_at
    from "warehouse"."marts_staging"."stg_contacts"
    where lower(coalesce(lifecycle_stage, '')) = 'customer'
),

deals_won as (
    -- Clients côté companies : deals gagnés
    select
        primary_company_id as company_id,
        null::bigint       as contact_id,
        'company'          as customer_type,
        closedate          as closed_at
    from "warehouse"."marts_staging"."stg_deals"
    where is_won = true
      and primary_company_id is not null
),

company_customers as (
    select
        null::bigint       as contact_id,
        company_id,
        customer_type,
        min(closed_at)     as first_seen_at
    from deals_won
    group by company_id, customer_type
),

unioned as (
    select * from customer_contacts
    union all
    select * from company_customers
)

select
    -- ID global pour éviter les collisions contact/company
    case
        when customer_type = 'contact'
            then 'contact_' || contact_id::text
        else 'company_' || company_id::text
    end as customer_global_id,

    customer_type,
    contact_id,
    company_id,
    first_seen_at
from unioned
  );