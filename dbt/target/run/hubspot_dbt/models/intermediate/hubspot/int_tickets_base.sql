
  create view "warehouse"."marts_intermediate"."int_tickets_base__dbt_tmp"
    
    
  as (
    

with tickets_src as (
    select
        *
    from "warehouse"."marts_staging"."stg_tickets"
),

/* ==============================
   1. DÉDUPLICATION DES TICKETS
   ============================== */

tickets_dedup as (
    select
        tickets_src.*,
        row_number() over (
            partition by ticket_id
            order by
                cast(updated_at as timestamp) desc,
                cast(extracted_at as timestamp) desc
        ) as rn
    from tickets_src
),

latest as (
    select *
    from tickets_dedup
    where rn = 1
),

/* ==============================
   2. PIPELINE (STAGES)
   ============================== */

pipeline as (
    select
        pipeline_id::bigint          as pipeline_id,
        pipeline_label,
        stage_id::bigint             as stage_id,
        stage_label,
        is_closed_stage::boolean     as is_closed_stage,
        ticket_state,
        display_order
    from "warehouse"."marts_staging"."stg_ticket_pipelines"
),

/* ==============================
   3. NOTES PAR TICKET
   ============================== */

notes_per_ticket as (
    select
        ticket_id::bigint as ticket_id,
        count(*) as note_count
    from "warehouse"."marts_staging"."stg_note_tickets"
    group by ticket_id
),

/* ==============================
   4. NORMALISATION & ENRICHISSEMENT
   ============================== */

tickets_normalized as (
    select
        l.ticket_id::bigint                    as ticket_id,
        l.subject,
        l.content,
        l.source_type,

        l.pipeline_id::bigint                 as pipeline_id,
        l.pipeline_stage_id::bigint           as pipeline_stage_id,

        l.is_closed::boolean                  as is_closed,

        cast(l.created_at as timestamp)       as created_at,
        cast(l.updated_at as timestamp)       as updated_at,
        cast(l.closed_date as timestamp)      as closed_at,
        cast(l.last_closed_date as timestamp) as last_closed_at,

        l.time_to_close_ms::bigint           as time_to_close_ms,

        l.created_by_contact_id::bigint      as created_by_contact_id,
        l.primary_company_id::bigint         as primary_company_id,
        l.primary_company_name,

        l.contacts_json,
        l.companies_json,

        cast(l.extracted_at as timestamp)    as extracted_at
    from latest l
),

tickets_with_pipeline as (
    select
        t.*,
        p.pipeline_label,
        p.stage_label,
        p.ticket_state,
        p.is_closed_stage,
        p.display_order as stage_display_order
    from tickets_normalized t
    left join pipeline p
        on t.pipeline_id = p.pipeline_id
       and t.pipeline_stage_id = p.stage_id
),

tickets_with_notes as (
    select
        t.*,
        coalesce(n.note_count, 0) as note_count
    from tickets_with_pipeline t
    left join notes_per_ticket n
        on t.ticket_id = n.ticket_id
),

/* ==============================
   5. DÉRIVÉS POUR KPI CS
   ============================== */

final as (
    select
        t.*,

        -- temps de résolution en jours (si fermé)
        case
            when t.is_closed = true
                 and t.time_to_close_ms is not null
            then t.time_to_close_ms / (1000.0 * 60 * 60 * 24)
            else null
        end as time_to_close_days,

        -- ticket réouvert si dernière date de fermeture > première date de fermeture
        case
            when t.closed_at is not null
                 and t.last_closed_at is not null
                 and t.last_closed_at > t.closed_at
            then true
            else false
        end as is_reopened,

        -- âge du ticket en jours (pour les tickets encore ouverts)
        case
            when t.is_closed = false
                 and t.created_at is not null
            then extract(epoch from (current_timestamp - t.created_at)) / 86400.0
            else null
        end as ticket_age_days
    from tickets_with_notes t
)

select *
from final
  );