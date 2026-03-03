{{ config(materialized='view') }}

with mgr as (
    select
        *,
        regexp_replace(
          regexp_replace(lower(trim(player_name)), '[^a-z0-9 ]', ''),
          '\\s+', ' '
        ) as player_name_key
    from {{ ref('stg_football_manager') }}
),

p as (
    select
        regexp_replace(
          regexp_replace(lower(trim(player_name)), '[^a-z0-9 ]', ''),
          '\\s+', ' '
        ) as player_name_key
    from {{ ref('dim_player') }}
)

select m.*
from mgr m
left join p
  on m.player_name_key = p.player_name_key
where p.player_name_key is null
