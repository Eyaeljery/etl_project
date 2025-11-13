with meetings as (
    select
        meeting_id,
        companies_json
    from {{ ref('stg_engagements_meetings') }}
),

exploded as (
    select
        m.meeting_id,
        (c)::text::bigint as company_id
    from meetings m
    cross join lateral jsonb_array_elements_text(m.companies_json) as c
)

select * from exploded

