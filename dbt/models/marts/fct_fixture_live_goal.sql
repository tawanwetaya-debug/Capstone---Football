{{ config(materialized='view') }}

select
  event_id,
  fixture_id,
  event_time,
  team_id,
  team_name,
  player_id as scorer_id,
  player_name as scorer_name,
  assist_id,
  assist_name,
  event_elapsed_min,
  event_extra_min,
  event_detail,
  event_comments
from {{ ref('fct_fixture_live_event') }}
where lower(event_type) = 'goal'