
  create view "warehouse"."marts_kpi"."kpi_sales_loss_rate_by_stage__dbt_tmp"
    
    
  as (
    

with deals as (
    select
        deal_stage_id,
        stage_label,
        deal_status
    from "warehouse"."marts_marts"."mart_sales_deals"
    where deal_stage_id is not null
      and deal_status in ('WON', 'LOST')
),

by_stage as (
    select
        deal_stage_id,
        stage_label,
        count(*) filter (where deal_status = 'LOST')                 as lost_count,
        count(*) filter (where deal_status in ('WON', 'LOST'))       as total_considered
    from deals
    group by
        deal_stage_id,
        stage_label
),

final as (
    select
        deal_stage_id,
        stage_label,
        lost_count,
        total_considered,
        case
            when total_considered > 0
            then lost_count::float / total_considered::float
            else null
        end as loss_rate
    from by_stage
)

select *
from final
  );