
  create view "warehouse"."marts_intermediate"."int_products__dbt_tmp"
    
    
  as (
    

with src as (
    select
        *
    from "warehouse"."marts_staging"."stg_products"
),

dedup as (
    select
        src.*,
        row_number() over (
            partition by product_id
            order by
                cast(updated_at_raw as timestamp) desc,
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
    product_id::bigint                as product_id,
    name                              as product_name,
    status,
    price::numeric                    as price,
    price_eur::numeric                as price_eur,

    cast(createdate as timestamp)     as created_at,
    cast(lastmodifieddate as timestamp) as last_modified_at,

    cast(created_at_raw as timestamp) as created_at_raw,
    cast(updated_at_raw as timestamp) as updated_at_raw,
    cast(extracted_at as timestamp)   as extracted_at
from latest
  );