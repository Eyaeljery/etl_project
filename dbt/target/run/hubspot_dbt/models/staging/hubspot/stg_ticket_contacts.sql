
  create view "warehouse"."marts_staging"."stg_ticket_contacts__dbt_tmp"
    
    
  as (
    with tickets as (
    select
        ticket_id,
        contacts_json
    from "warehouse"."marts_staging"."stg_tickets"
),

exploded as (
    select
        t.ticket_id,
        (c)::text::bigint as contact_id
    from tickets t
    cross join lateral jsonb_array_elements_text(t.contacts_json) as c
)

select * from exploded
  );