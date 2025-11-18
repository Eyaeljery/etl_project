

with src as (
    select
        *
    from "warehouse"."marts_staging"."stg_form_submissions"
),

dedup as (
    select
        src.*,
        row_number() over (
            partition by conversion_id
            order by
                cast(updated_at as timestamp) desc,
                cast(extracted_at as timestamp) desc
        ) as rn
    from src
),

latest as (
    select *
    from dedup
    where rn = 1
)

select
    conversion_id                     as submission_id,
    form_id,
    page_url,

    cast(submitted_at as timestamp)   as submitted_at,
    cast(updated_at as timestamp)     as updated_at,
    cast(extracted_at as timestamp)   as extracted_at,

    email,
    first_name,
    last_name,

    values_json
from latest