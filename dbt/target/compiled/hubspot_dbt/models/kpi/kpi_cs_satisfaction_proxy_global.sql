

with base as (
    select
        satisfaction_proxy,
        coalesce(is_customer_ticket, false) as is_customer_ticket
    from "warehouse"."marts_marts"."mart_cs_tickets"
    where satisfaction_proxy is not null
),

overall as (
    select
        count(*) as tickets_total,
        count(*) filter (where satisfaction_proxy = 'HIGH')   as high_count,
        count(*) filter (where satisfaction_proxy = 'MEDIUM') as medium_count,
        count(*) filter (where satisfaction_proxy = 'LOW')    as low_count
    from base
),

customers_only as (
    select
        count(*) as tickets_total_customers,
        count(*) filter (where satisfaction_proxy = 'HIGH')   as high_count_customers,
        count(*) filter (where satisfaction_proxy = 'MEDIUM') as medium_count_customers,
        count(*) filter (where satisfaction_proxy = 'LOW')    as low_count_customers
    from base
    where is_customer_ticket = true
),

final as (
    select
        o.tickets_total,
        o.high_count,
        o.medium_count,
        o.low_count,
        case when o.tickets_total > 0
             then o.high_count::float / o.tickets_total::float
             else null
        end as high_ratio,
        case when o.tickets_total > 0
             then o.medium_count::float / o.tickets_total::float
             else null
        end as medium_ratio,
        case when o.tickets_total > 0
             then o.low_count::float / o.tickets_total::float
             else null
        end as low_ratio,

        c.tickets_total_customers,
        c.high_count_customers,
        c.medium_count_customers,
        c.low_count_customers,
        case when c.tickets_total_customers > 0
             then c.high_count_customers::float / c.tickets_total_customers::float
             else null
        end as high_ratio_customers,
        case when c.tickets_total_customers > 0
             then c.medium_count_customers::float / c.tickets_total_customers::float
             else null
        end as medium_ratio_customers,
        case when c.tickets_total_customers > 0
             then c.low_count_customers::float / c.tickets_total_customers::float
             else null
        end as low_ratio_customers
    from overall o
    cross join customers_only c
)

select *
from final