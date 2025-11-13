with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from {{ source('hubspot', 'raw_hubspot_raw__stream_deal_pipelines') }}
),

pipelines as (
    select
        (j->>'id')         as pipeline_id,
        (j->>'label')      as pipeline_label,
        extracted_at,
        j->'stages'        as stages_json
    from src
),

stages as (
    select
        p.pipeline_id,
        p.pipeline_label,
        (s->>'id')          as stage_id,
        (s->>'label')       as stage_label,
        coalesce((s->'metadata'->>'isClosed')::boolean, false) as is_closed_stage,
        (s->'metadata'->>'probability')::numeric                as stage_probability,
        (s->>'displayOrder')::int                               as display_order,
        p.extracted_at
    from pipelines p
    left join lateral jsonb_array_elements(p.stages_json) as s on true
)

select *
from stages
