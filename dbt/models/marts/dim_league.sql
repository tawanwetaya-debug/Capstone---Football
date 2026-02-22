{{ config(materialized='table') }}

with f as (
  select

    league_id,
    country_name,
    league_name,
    max(ingested_at) as ingested_at
  from {{ ref('stg_league') }}
  group by 1,2,3
),

-- clean up duplicate and null values
cleanned as (
  select
  league_id,
  nullif(trim(country_name), '') as league_country,
  nullif(trim(league_name), '') as league_name,
  ingested_at,
  row_number() over (
    partition by league_id
    order by ingested_at desc
  ) as row_num

from f
where league_id is not null
)


select
    league_id,
    league_country,
    league_name,
    current_timestamp as created_at,
    current_timestamp as updated_at
  
from cleanned
where row_num = 1