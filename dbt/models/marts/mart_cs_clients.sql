{{ config(
    materialized = 'view'
) }}

with contacts as (
    select
        contact_pk,
        contact_id,
        email,
        full_name,
        lifecycle_stage,
        is_customer,
        became_customer_at,
        became_customer_date,
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name
    from {{ ref('mart_marketing_leads') }}
),

/* ===========================================
   1. AGRÉGATS TICKETS PAR CONTACT
   (int_ticket_contacts_agg)
   =========================================== */

tickets_agg as (
    select
        contact_id::bigint                 as contact_id,
        tickets_count,
        tickets_closed_count,
        tickets_open_count,
        tickets_reopened_count,
        avg_time_to_close_days,
        has_recurring_tickets,
        first_ticket_created_at,
        last_ticket_created_at
    from {{ ref('int_ticket_contacts_agg') }}
),

/* ===========================================
   2. AGGRÉGATION SATISFACTION PAR CONTACT
   (à partir des tickets enrichis)
   =========================================== */

ticket_contacts as (
    select
        ticket_id::bigint  as ticket_id,
        contact_id::bigint as contact_id
    from {{ ref('stg_ticket_contacts') }}
),

tickets as (
    select
        ticket_id,
        satisfaction_proxy
    from {{ ref('mart_cs_tickets') }}
),

tickets_with_contacts as (
    select
        tc.contact_id,
        t.ticket_id,
        t.satisfaction_proxy
    from ticket_contacts tc
    join tickets t
        on tc.ticket_id = t.ticket_id
),

satisfaction_agg as (
    select
        contact_id,
        count(distinct ticket_id)                                     as tickets_with_cs_count,
        count(*) filter (where satisfaction_proxy = 'HIGH')           as tickets_high_sat_count,
        count(*) filter (where satisfaction_proxy = 'MEDIUM')         as tickets_medium_sat_count,
        count(*) filter (where satisfaction_proxy = 'LOW')            as tickets_low_sat_count
    from tickets_with_contacts
    group by contact_id
),

/* ===========================================
   3. JOIN CONTACTS + TICKETS
   =========================================== */

joined as (
    select
        c.contact_pk,
        c.contact_id,
        c.email,
        c.full_name,
        c.lifecycle_stage,
        c.is_customer,
        c.became_customer_at,
        c.became_customer_date,
        c.owner_id,
        c.owner_email,
        c.owner_first_name,
        c.owner_last_name,

        ta.tickets_count,
        ta.tickets_closed_count,
        ta.tickets_open_count,
        ta.tickets_reopened_count,
        ta.avg_time_to_close_days,
        ta.has_recurring_tickets,
        ta.first_ticket_created_at,
        ta.last_ticket_created_at,

        sa.tickets_with_cs_count,
        sa.tickets_high_sat_count,
        sa.tickets_medium_sat_count,
        sa.tickets_low_sat_count
    from contacts c
    left join tickets_agg ta
        on cast(c.contact_id as bigint) = ta.contact_id
    left join satisfaction_agg sa
        on cast(c.contact_id as bigint) = sa.contact_id
),

final as (
    select
        contact_pk,
        contact_id,
        email,
        full_name,

        lifecycle_stage,
        is_customer,
        became_customer_at,
        became_customer_date,

        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,

        coalesce(tickets_count, 0)          as tickets_count,
        coalesce(tickets_closed_count, 0)   as tickets_closed_count,
        coalesce(tickets_open_count, 0)     as tickets_open_count,
        coalesce(tickets_reopened_count, 0) as tickets_reopened_count,
        avg_time_to_close_days,
        coalesce(has_recurring_tickets, false) as has_recurring_tickets,
        first_ticket_created_at,
        last_ticket_created_at,

        coalesce(tickets_with_cs_count, 0)      as tickets_with_cs_count,
        coalesce(tickets_high_sat_count, 0)     as tickets_high_sat_count,
        coalesce(tickets_medium_sat_count, 0)   as tickets_medium_sat_count,
        coalesce(tickets_low_sat_count, 0)      as tickets_low_sat_count,

        case
            when coalesce(tickets_with_cs_count, 0) > 0
            then tickets_high_sat_count::float / tickets_with_cs_count
            else null
        end as high_satisfaction_ratio
    from joined
)

select *
from final