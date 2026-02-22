{{ config(materialized='table') }}

with f as (
  select

    league_id,
    league_country,
    league_name,

    max(ingested_at) as ingested_at
  from {{ ref('stg_league') }}
  group by 1,2,3,4
)

select
    league_id,
    league_country,
    league_name,
  current_timestamp as created_at,
  current_timestamp as updated_at
  
from f