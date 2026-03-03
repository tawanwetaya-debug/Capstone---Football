{{ config(materialized='table') }}

with p as (
  select
    player_id,
    player_name,
    regexp_replace(
      regexp_replace(lower(trim(player_name)), '[^a-z0-9 ]', ''),
      '\\s+', ' '
    ) as player_name_key
  from {{ ref('dim_player') }}
  where player_name is not null
),

mgr as (
  select
    *,
    regexp_replace(
      regexp_replace(lower(trim(player_name)), '[^a-z0-9 ]', ''),
      '\\s+', ' '
    ) as player_name_key
  from {{ ref('stg_football_manager') }}
  where player_name is not null
),

joined as (
  select
    p.player_id,
    p.player_name as dim_player_name,
    m.*,
    row_number() over (
      partition by p.player_id
      order by m.ingested_at desc
    ) as rn
  from p
  left join mgr m
    on p.player_name_key = m.player_name_key
)

select *
from joined
where rn = 1 and player_name is not null