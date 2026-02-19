{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        try_parse_json(payload) as p
    from {{ source('football_capstone', 'RAW_PLAYERS_STATISTICS') }}

)


select
    ingested_at,
    league_id,
    season,
    p:statistics[0]:team:id::number        as team_id,
    p:player:id::number        as player_id,
    p:player:firstname::string as player_first_name,
    p:player:lastname::string  as player_last_name,
    p:player:age::number       as player_age,
    p:player:nationality::string  as player_nationality,
    p:player:photo::string      as player_photo_url,

    p:statistics[0]:team                  as team_json,
    p:statistics[0]:league                as league_json,
    p:statistics[0]:games                 as games_json,
    p:statistics[0]:substitutes           as substitutes_json,
    p:statistics[0]:shots                 as shots_json,
    p:statistics[0]:goals                 as goals_json,
    p:statistics[0]:passes                as passes_json,
    p:statistics[0]:tackles               as tackles_json,
    p:statistics[0]:duels                 as duels_json,
    p:statistics[0]:dribbles              as dribbles_json,
    p:statistics[0]:fouls                 as fouls_json,
    p:statistics[0]:cards                 as cards_json,
    p:statistics[0]:penalty               as penalty_json

from src
where p is not null
  