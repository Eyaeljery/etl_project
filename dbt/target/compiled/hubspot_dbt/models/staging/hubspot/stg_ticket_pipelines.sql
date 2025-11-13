with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from warehouse.airbyte_internal.raw_hubspot_raw__stream_ticket_pipelines
),

pipelines as (
    select
        (j->>'id')      as pipeline_id,
        (j->>'label')   as pipeline_label,
        j->'stages'     as stages_json,
        extracted_at
    from src
),

stages as (
    select
        p.pipeline_id,
        p.pipeline_label,

        (s->>'id')      as stage_id,
        (s->>'label')   as stage_label,
        coalesce((s->'metadata'->>'isClosed')::boolean, false) as is_closed_stage,
        (s->'metadata'->>'ticketState') as ticket_state,

        (s->>'displayOrder')::int as display_order,
        extracted_at
    from pipelines p
    cross join lateral jsonb_array_elements(p.stages_json) as s
)

select * from stages