{{ config(materialized='view') }}

select
  event_id,
  fixture_id,
  event_time,
  team_id,
  team_name,
  player_id,
  player_name,
  event_elapsed_min,
  event_extra_min,
  event_detail,
  case
    when lower(event_detail) like '%red%' then 'red'
    when lower(event_detail) like '%yellow%' then 'yellow'
    else 'unknown'
  end as card_color
from {{ ref('fct_fixture_live_event') }}
where lower(event_type) = 'card'