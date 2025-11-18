{{ config(
    materialized = 'view'
) }}

with ticket_contacts as (
    select
        ticket_id::bigint  as ticket_id,
        contact_id::bigint as contact_id
    from {{ ref('stg_ticket_contacts') }}
),

tickets as (
    select
        ticket_id,
        created_at,
        closed_at,
        is_closed,
        is_reopened,
        time_to_close_days
    from {{ ref('int_tickets_base') }}
),

joined as (
    select
        tc.contact_id,
        t.ticket_id,
        t.created_at,
        t.closed_at,
        t.is_closed,
        t.is_reopened,
        t.time_to_close_days
    from ticket_contacts tc
    left join tickets t
        on tc.ticket_id = t.ticket_id
),

agg as (
    select
        contact_id,

        count(distinct ticket_id)                          as tickets_count,
        count(distinct ticket_id) filter (where is_closed) as tickets_closed_count,
        count(distinct ticket_id) filter (where not is_closed) as tickets_open_count,

        min(created_at)                                    as first_ticket_created_at,
        max(created_at)                                    as last_ticket_created_at,

        -- tickets réouverts
        count(distinct ticket_id) filter (where is_reopened) as tickets_reopened_count,

        -- temps moyen de résolution par client (sur tickets fermés)
        avg(time_to_close_days) filter (where is_closed)   as avg_time_to_close_days
    from joined
    group by contact_id
)

select
    a.*,
    case
        when tickets_count > 1 then true
        else false
    end as has_recurring_tickets
from agg a