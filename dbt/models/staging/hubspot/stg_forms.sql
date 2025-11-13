with src as (
    select
        (_airbyte_data)::jsonb as j,
        _airbyte_extracted_at  as extracted_at
    from {{ source('hubspot', 'raw_hubspot_raw__stream_forms') }}
)

select
    (j->>'id')                               as form_id,
    (j->>'name')                             as form_name,
    (j->>'formType')                         as form_type,
    nullif(j->>'createdAt','')::timestamptz as created_at,
    nullif(j->>'updatedAt','')::timestamptz as updated_at,

    (j->'configuration'->>'language')        as language,
    (j->'configuration'->>'notifyContactOwner')::boolean as notify_contact_owner,
    (j->'configuration'->>'postSubmitAction')            as post_submit_action_json,

    extracted_at
from src

