{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        fixture_id,
        try_parse_json(payload) as p
    from {{ source('football_capstone', 'RAW_FIXTURE_LINE_UP') }}
)

select
    ingested_at,
    league_id,
    season,
    fixture_id,

    p:team:id::number          as team_id,
    p:team:name::string        as team_name,
    p:team:logo::string        as team_logo_url,
    p:team:colors              as team_colors,

    p:coach:id::number         as coach_id,
    p:coach:name::string       as coach_name,
    p:coach:photo::string      as coach_photo_url,

    p:formation::string        as formation,

    p:startXI                  as start_xi_json,
    p:substitutes              as substitutes_json,

    array_size(p:startXI)      as start_xi_cnt,
    array_size(p:substitutes)  as substitutes_cnt

from src
where p is not null