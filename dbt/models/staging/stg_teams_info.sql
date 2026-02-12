{{ config(materialized='view') }}

with src as (
  select
    ingested_at,
    league_id,
    team_id,
    season,
    try_parse_json(payload) as p
  from {{ source('football_capstone', 'raw_teams_info') }}
)

select
  ingested_at,
  league_id,
  season,
  team_id,

  p:team:id::number        as api_team_id,
  p:team:name::string      as team_name,
  p:team:code::string      as team_code,
  p:team:country::string   as team_country,
  p:team:founded::number   as team_founded,
  p:team:national::boolean as is_national_team,
  p:team:logo::string      as team_logo_url,

  p:venue:id::number       as venue_id,
  p:venue:name::string     as venue_name,
  p:venue:address::string  as venue_address,
  p:venue:city::string     as venue_city,
  p:venue:capacity::number as venue_capacity,
  p:venue:surface::string  as venue_surface,
  p:venue:image::string    as venue_image_url,

  -- keep json
  p as payload_json

from src
where p is not null