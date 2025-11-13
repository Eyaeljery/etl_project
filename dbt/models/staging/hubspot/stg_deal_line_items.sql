with src as (
  select
    (_airbyte_data)::jsonb as j
  from {{ source('hubspot', 'raw_hubspot_raw__stream_deals') }}
),
bridge as (
  select
    (j->>'id')::bigint as deal_id,
    nullif(x.value::text,'"') as line_item_id_text
  from src
  left join lateral jsonb_array_elements_text(j->'line_items') as x(value) on true
)
select
  deal_id,
  nullif(replace(line_item_id_text,'"',''),'')::bigint as line_item_id
from bridge
where line_item_id_text is not null and line_item_id_text <> ''
