{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        fixture_id,
        try_parse_json(payload) as p
    from {{ source('football_capstone', 'RAW_FIXTURE_PLAYERS_STATISTIC') }}
),

-- 1 row per player
players as (
    select
        s.ingested_at,
        s.league_id,
        s.season,
        s.fixture_id,
        s.p:team:id::number     as team_id,
        s.p:team:name::string   as team_name,
        pl.value                as player_obj
    from src s,
         lateral flatten(input => s.p:players) pl
),

-- 1 row per player per statistics-entry
stats as (
    select
        p.ingested_at,
        p.league_id,
        p.season,
        p.fixture_id,
        p.team_id,
        p.team_name,

        p.player_obj:player:id::number     as player_id,
        p.player_obj:player:name::string   as player_name,
        p.player_obj:player:photo::string  as player_photo_url,

        st.value                           as stat
    from players p,
         lateral flatten(input => p.player_obj:statistics) st
),

final as (
    select
        ingested_at,
        league_id,
        season,
        fixture_id,
        team_id,
        team_name,

        player_id,
        player_name,
        player_photo_url,

        -- keep the whole statistics object as json/variant
        stat:cards      as cards_json,
        stat:dribbles   as dribbles_json,
        stat:duels      as duels_json,
        stat:fouls      as fouls_json,
        stat:games      as games_json,
        stat:goals      as goals_json,
        try_to_number(stat:offsides::string)   as offsides,
        stat:passes     as passes_json,
        stat:penalty    as penalty_json,
        stat:shots      as shots_json,
        stat:tackles    as tackles_json

    from stats
)

select * from final