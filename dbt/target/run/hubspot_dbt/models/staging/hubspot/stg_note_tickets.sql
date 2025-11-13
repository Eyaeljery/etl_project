
  create view "warehouse"."marts_staging"."stg_note_tickets__dbt_tmp"
    
    
  as (
    with notes as (
    select
        note_id,
        tickets_json
    from "warehouse"."marts_staging"."stg_engagements_notes"
),

exploded as (
    select
        n.note_id,
        (t)::text::bigint as ticket_id
    from notes n
    cross join lateral jsonb_array_elements_text(n.tickets_json) as t
)

select * from exploded
  );