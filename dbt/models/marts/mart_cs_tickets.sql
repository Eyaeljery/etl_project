{{ config(
    materialized = 'view'
) }}

with tickets as (
    select
        ticket_id,
        subject,
        content,
        source_type,

        pipeline_id,
        pipeline_label,
        pipeline_stage_id,
        stage_label,
        ticket_state,
        is_closed_stage,
        stage_display_order,

        is_closed,
        created_at,
        updated_at,
        closed_at,
        last_closed_at,
        time_to_close_ms,
        time_to_close_days,
        is_reopened,
        ticket_age_days,

        created_by_contact_id,
        primary_company_id,
        primary_company_name,

        contacts_json,
        companies_json,

        note_count,
        extracted_at
    from {{ ref('int_tickets_base') }}
),

/* ===================================
   1. CONTACTS LIÉS À CHAQUE TICKET
   =================================== */

ticket_contacts as (
    select
        ticket_id::bigint  as ticket_id,
        contact_id::bigint as contact_id
    from {{ ref('stg_ticket_contacts') }}
),

ticket_contacts_agg as (
    select
        ticket_id,
        count(distinct contact_id)           as contacts_count,
        min(contact_id)                      as primary_contact_id  -- choix simple : le plus petit ID
    from ticket_contacts
    group by ticket_id
),

/* ===================================
   2. ENRICHISSEMENT AVEC LE CONTACT
   =================================== */

contacts as (
    select
        contact_pk,
        contact_id,
        email,
        full_name,
        lifecycle_stage,
        became_customer_at,
        became_lead_at
    from {{ ref('int_contacts_base') }}
),

tickets_with_contact as (
    select
        t.*,
        tc.contacts_count,
        tc.primary_contact_id,

        c.contact_pk             as primary_contact_pk,
        c.email                  as primary_contact_email,
        c.full_name              as primary_contact_name,
        c.lifecycle_stage        as primary_contact_lifecycle_stage,
        c.became_customer_at     as primary_contact_became_customer_at,
        c.became_lead_at         as primary_contact_became_lead_at
    from tickets t
    left join ticket_contacts_agg tc
        on t.ticket_id = tc.ticket_id
    left join contacts c
        on tc.primary_contact_id = c.contact_id
),

/* ===================================
   3. PROXY DE SATISFACTION
   =================================== */

final as (
    select
        ticket_id,

        -- info ticket
        subject,
        content,
        source_type,

        pipeline_id,
        pipeline_label,
        pipeline_stage_id,
        stage_label,
        ticket_state,
        is_closed_stage,
        stage_display_order,

        is_closed,
        is_reopened,
        created_at,
        updated_at,
        closed_at,
        last_closed_at,
        time_to_close_ms,
        time_to_close_days,
        ticket_age_days,

        note_count,

        -- contacts liés
        contacts_count,
        primary_contact_id,
        primary_contact_pk,
        primary_contact_email,
        primary_contact_name,
        primary_contact_lifecycle_stage,
        primary_contact_became_customer_at,
        primary_contact_became_lead_at,

        -- flags utiles
        case
            when primary_contact_became_customer_at is not null then true
            else false
        end as is_customer_ticket,

        -- proxy très simple de satisfaction (à adapter si besoin)
        case
            when is_closed = true
             and coalesce(time_to_close_days, 9999) <= 1
             and is_reopened = false
                then 'HIGH'
            when is_closed = true
             and coalesce(time_to_close_days, 9999) <= 3
             and is_reopened = false
                then 'MEDIUM'
            else 'LOW'
        end as satisfaction_proxy,
        
        extracted_at
    from tickets_with_contact
)

select *
from final