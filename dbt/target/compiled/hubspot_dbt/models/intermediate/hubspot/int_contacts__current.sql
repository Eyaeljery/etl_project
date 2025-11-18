with base as (
    select
        contact_id,
        created_at,
        updated_at,
        email,
        first_name,
        last_name,
        country,

        lifecycle_stage,
        lead_status,
        marketable_status,
        owner_id,

        lifecyclestage_lead_date,
        lifecyclestage_opportunity_date,
        lifecyclestage_customer_date,

        analytics_source,
        analytics_first_url,
        analytics_first_referrer,
        num_page_views,
        num_visits,
        first_analytics_ts,
        last_analytics_ts,
        first_touch_campaign,
        last_touch_campaign,

        companies_json,
        extracted_at
    from "warehouse"."marts_staging"."stg_contacts"
),

normalized as (
    select
        contact_id,
        email,
        first_name,
        last_name,
        country,

        lifecycle_stage,
        lead_status,
        marketable_status,
        owner_id,

        -- dates raw
        created_at,
        updated_at,
        lifecyclestage_lead_date,
        lifecyclestage_opportunity_date,
        lifecyclestage_customer_date,

        -- date de lead : on prend la property, sinon, si le contact est déjà au moins "lead",
        -- on fallback sur la date de création
        case
            when lifecyclestage_lead_date is not null then lifecyclestage_lead_date
            when lifecycle_stage in ('lead','marketingqualifiedlead','salesqualifiedlead','opportunity','customer')
                 then created_at
            else null
        end as lead_date,

        -- bucket mensuel pour le KPI
        case
            when lifecyclestage_lead_date is not null then date_trunc('month', lifecyclestage_lead_date)::date
            when lifecycle_stage in ('lead','marketingqualifiedlead','salesqualifiedlead','opportunity','customer')
                 then date_trunc('month', created_at)::date
            else null
        end as lead_month,

        -- date de customer si dispo
        lifecyclestage_customer_date as customer_date,
        case
            when lifecyclestage_customer_date is not null
                then date_trunc('year', lifecyclestage_customer_date)::date
            else null
        end as customer_year,

        -- flags rapides
        case
            when lifecycle_stage in ('lead','marketingqualifiedlead','salesqualifiedlead','opportunity','customer')
                then true else false
        end as is_lead_or_more,

        case
            when lifecycle_stage = 'customer' then true else false
        end as is_customer,

        -- analytics
        analytics_source,
        analytics_first_url,
        analytics_first_referrer,
        num_page_views,
        num_visits,
        first_analytics_ts,
        last_analytics_ts,
        first_touch_campaign,
        last_touch_campaign,

        companies_json,
        extracted_at
    from base
)

select * from normalized