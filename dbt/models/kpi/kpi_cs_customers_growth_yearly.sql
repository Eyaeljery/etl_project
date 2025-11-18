{{ config(
    materialized = 'view'
) }}

with customers as (
    select
        contact_pk,
        became_customer_at
    from {{ ref('mart_marketing_leads') }}
    where is_customer = true
      and became_customer_at is not null
),

by_year as (
    select
        date_trunc('year', became_customer_at) as year_start,
        count(distinct contact_pk)             as new_customers_count
    from customers
    group by date_trunc('year', became_customer_at)
),

final as (
    select
        year_start,
        new_customers_count,
        sum(new_customers_count) over (
            order by year_start
            rows between unbounded preceding and current row
        ) as total_customers_cumulative
    from by_year
)

select *
from final