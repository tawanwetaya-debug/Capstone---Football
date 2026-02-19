{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        fixture_id,
        try_parse_json(payload) as p
    from {{ source('football_capstone', 'RAW_FIXTURE_PREDICTIONS') }}
),


final as (
    select
        ingested_at,
        league_id,
        season,
        fixture_id,

        p:predictions:advice::string as advice,

        -- percent values are like "45%" so strip "%"
        try_to_number(replace(p:predictions:percent:away::string, '%','')) as percent_away,
        try_to_number(replace(p:predictions:percent:home::string, '%','')) as percent_home,
        try_to_number(replace(p:predictions:percent:draw::string, '%','')) as percent_draw,

        p:teams:away:id::number      as team_away_id,
        p:teams:away:name::string    as team_away_name,

        p:teams:home:id::number      as team_home_id,
        p:teams:home:name::string    as team_home_name,

        -- keep as VARIANT (JSON)
        p:teams:away:last_5          as team_away_last_5,
        p:teams:home:last_5          as team_home_last_5,

        p:teams:away:league          as team_away_league,
        p:teams:home:league          as team_home_league,

        p as payload_json

    from src
    where p is not null
)

select * from final



