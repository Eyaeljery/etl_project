
  create view "warehouse"."marts_kpi"."kpi_cs_tickets_resolution_monthly__dbt_tmp"
    
    
  as (
    

with tickets as (
    select
        created_at,
        closed_at,
        is_closed
    from "warehouse"."marts_marts"."mart_cs_tickets"
    where created_at is not null
),

-- 1️⃣ Tickets créés par mois (basé sur created_at)
tickets_created as (
    select
        date_trunc('month', created_at) as month_start,
        count(*)                        as tickets_created_count
    from tickets
    group by date_trunc('month', created_at)
),

-- 2️⃣ Tickets FERMÉS par mois (basé sur closed_at)
tickets_closed as (
    select
        date_trunc('month', closed_at) as month_start,
        count(*)                       as tickets_closed_count
    from tickets
    where is_closed = true
      and closed_at is not null
    group by date_trunc('month', closed_at)
),

-- 3️⃣ Calendrier des mois présents soit en création soit en fermeture
months as (
    select month_start from tickets_created
    union
    select month_start from tickets_closed
),

-- 4️⃣ Jointure et calcul du taux
monthly as (
    select
        m.month_start,
        coalesce(c.tickets_created_count, 0) as tickets_created_count,
        coalesce(cl.tickets_closed_count, 0) as tickets_closed_count
    from months m
    left join tickets_created c
        on m.month_start = c.month_start
    left join tickets_closed cl
        on m.month_start = cl.month_start
),

final as (
    select
        month_start,
        tickets_created_count,
        tickets_closed_count,
        case
            when tickets_created_count > 0
            then tickets_closed_count::float / tickets_created_count::float
            else null
        end as tickets_resolution_rate
    from monthly
)

select *
from final
order by month_start
  );