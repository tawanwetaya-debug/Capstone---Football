{{ config(materialized='view') }}

select
  event_id,
  fixture_id,
  event_time,
  team_id,
  team_name,
  event_elapsed_min,
  event_extra_min,
  player_id as player_out_id,
  player_name as player_out_name,
  assist_id as player_in_id,
  assist_name as player_in_name,
  event_detail
from {{ ref('fct_fixture_live_event') }}
where lower(event_type) in ('subst', 'substitution')