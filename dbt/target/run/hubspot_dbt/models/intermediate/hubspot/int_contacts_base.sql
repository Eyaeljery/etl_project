
  create view "warehouse"."marts_intermediate"."int_contacts_base__dbt_tmp"
    
    
  as (
    

with src as (
    select
        *
    from "warehouse"."marts_staging"."stg_contacts"
),

-- certains contacts apparaissent plusieurs fois dans la table de staging
dedup as (
    select
        src.*,
        row_number() over (
            partition by contact_id
            order by cast(updated_at as timestamp) desc,
                     cast(extracted_at as timestamp) desc
        ) as rn
    from src
),

latest as (
    select *
    from dedup
    where rn = 1
),

base as (
    select
        contact_id                                    as contact_pk,
        contact_id                                    as contact_id,
        email,
        first_name,
        last_name,
        trim(concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))) as full_name,
        country,
        lifecycle_stage,
        lead_status,
        marketable_status                            as is_marketable,
        owner_id,

        -- dates principales
        cast(created_at as timestamp)                as created_at,
        cast(updated_at as timestamp)                as updated_at,
        cast(lifecyclestage_lead_date as timestamp)         as became_lead_at,
        cast(lifecyclestage_opportunity_date as timestamp)  as became_opportunity_at,
        cast(lifecyclestage_customer_date as timestamp)     as became_customer_at,

        -- analytics & attribution
        analytics_source,
        analytics_first_url,
        analytics_first_referrer,
        num_page_views,
        num_visits,
        cast(first_analytics_ts as timestamp)        as first_analytics_at,
        cast(last_analytics_ts as timestamp)         as last_analytics_at,
        first_touch_campaign,
        last_touch_campaign,

        -- associations brutes (JSON) vers les companies, utile plus tard
        companies_json,

        cast(extracted_at as timestamp)              as extracted_at
    from latest
)

select
    b.*,
    o.email      as owner_email,
    o.first_name as owner_first_name,
    o.last_name  as owner_last_name,
    o.is_archived as owner_is_archived
from base b
left join "warehouse"."marts_intermediate"."int_owners" o
    on b.owner_id = o.owner_id
  );