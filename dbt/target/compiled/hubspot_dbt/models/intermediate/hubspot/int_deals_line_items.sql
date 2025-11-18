

with src as (
    select
        deal_id,
        line_item_id
    from "warehouse"."marts_staging"."stg_deal_line_items"
),

-- On enlève simplement les doublons
distinct_links as (
    select distinct
        deal_id::bigint       as deal_id,
        line_item_id::bigint  as line_item_id
    from src
),

-- Enrichissement optionnel avec le deal (pratique en analyse)
deals as (
    select
        deal_id,
        deal_name,
        owner_id,
        created_date,
        closed_date,
        deal_status,
        hs_closed_amount_in_home_currency
    from "warehouse"."marts_intermediate"."int_deals_base"
)

select
    dl.deal_id,
    dl.line_item_id,
    d.deal_name,
    d.owner_id,
    d.created_date,
    d.closed_date,
    d.deal_status,
    d.hs_closed_amount_in_home_currency
from distinct_links dl
left join deals d
    on dl.deal_id = d.deal_id