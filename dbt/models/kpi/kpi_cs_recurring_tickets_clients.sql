{{ config(
    materialized = 'view'
) }}

with clients as (
    select
        contact_pk,
        contact_id,
        email,
        full_name,
        lifecycle_stage,
        is_customer,
        became_customer_at,
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        tickets_count,
        tickets_closed_count,
        tickets_open_count,
        tickets_reopened_count,
        has_recurring_tickets,
        first_ticket_created_at,
        last_ticket_created_at
    from {{ ref('mart_cs_clients') }}
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
        owner_id,
        owner_email,
        owner_first_name,
        owner_last_name,
        coalesce(tickets_count, 0)          as tickets_count,
        coalesce(tickets_closed_count, 0)   as tickets_closed_count,
        coalesce(tickets_open_count, 0)     as tickets_open_count,
        coalesce(tickets_reopened_count, 0) as tickets_reopened_count,
        coalesce(has_recurring_tickets, false) as has_recurring_tickets,
        first_ticket_created_at,
        last_ticket_created_at
    from clients
)

select *
from final