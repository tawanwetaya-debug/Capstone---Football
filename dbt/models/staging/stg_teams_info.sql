{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        team_id,
        season,
        payload
    from {{ source('football_capstone', 'raw_teams_info') }}
),

-- 1) explode payload.response[]
resp as (

    select
        s.ingested_at,
        s.league_id,
        s.team_id,
        s.season,
        r.value as resp_value
    from src s,
         lateral flatten(input => s.payload:response) r

),

final as (
    select
        ingested_at,
        league_id,
        season,
        team_id,

        -- TEAM (from response.team)
        resp_value:team:id::number              as api_team_id,
        resp_value:team:name::string            as team_name,
        resp_value:team:code::string            as team_code,
        resp_value:team:country::string         as team_country,
        resp_value:team:founded::number         as team_founded,
        resp_value:team:national::boolean       as is_national_team,
        resp_value:team:logo::string            as team_logo_url,

        -- VENUE (from response.venue)
        resp_value:venue:id::number             as venue_id,
        resp_value:venue:name::string           as venue_name,
        resp_value:venue:address::string        as venue_address,
        resp_value:venue:city::string           as venue_city,
        resp_value:venue:capacity::number       as venue_capacity,
        resp_value:venue:surface::string        as venue_surface,
        resp_value:venue:image::string          as venue_image_url

    from resp
)

select * from final
