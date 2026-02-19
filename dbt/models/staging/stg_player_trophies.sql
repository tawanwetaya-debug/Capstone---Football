{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        try_parse_json(payload) as p
    from {{ source('football_capstone', 'RAW_PLAYER_TROPHIES') }}
),

trophies as (
    select
        s.ingested_at,
        s.league_id,
        s.season,
        s.p:id::number as player_id,
        s.p as payload_json,
        t.index + 1 as trophy_n,
        t.value as trophy
    from src s,
         lateral flatten(input => s.p:trophies) t
    where s.p is not null
),

ranked as (
    select
        *,
        row_number() over (partition by player_id order by trophy_n) as rn
    from trophies
    qualify rn <= 20
),

pivoted as (
    select
        player_id,
        max(league_id) as league_id,
        max(payload_json) as payload_json,

        max(case when rn = 1 then trophy:league::string end)  as trophy_1_league,
        max(case when rn = 1 then trophy:season::string end)  as trophy_1_season,
        max(case when rn = 1 then trophy:place::string end)   as trophy_1_place,

        max(case when rn = 2 then trophy:league::string end)  as trophy_2_league,
        max(case when rn = 2 then trophy:season::string end)  as trophy_2_season,
        max(case when rn = 2 then trophy:place::string end)   as trophy_2_place,

        max(case when rn = 3 then trophy:league::string end)  as trophy_3_league,
        max(case when rn = 3 then trophy:season::string end)  as trophy_3_season,
        max(case when rn = 3 then trophy:place::string end)   as trophy_3_place,
        

    from ranked
    group by player_id
)

select * from pivoted
