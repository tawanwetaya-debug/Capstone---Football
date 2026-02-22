{{ config(materialized='table') }}

with src as (
  select
    fixture_id,
    league_id,
    season,
    ingested_at
  from {{ ref('stg_fixture_info') }}
),

dedup as (
  select
    *,
    row_number() over (
      partition by fixture_id
      order by ingested_at desc
    ) as rn
  from src
)

select
  fixture_id,
  league_id,
  season,
  current_timestamp as created_at,
  current_timestamp as updated_at
from dedup
where rn = 1