{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        payload
    from {{ source('football_capstone', 'raw_league') }}
),

-- 1) explode payload.response[]
resp as (

    select
        s.ingested_at,
        s.league_id,
        s.season,
        r.value as resp_value
    from src s,
         lateral flatten(input => s.payload:response) r

),

-- 2) explode seasons[]
season_rows as (

    select
        r.ingested_at,
        r.league_id,
        r.season,

        -- league fields
        r.resp_value:league:id::number           as league_id_from_payload,
        r.resp_value:league:name::string         as league_name,
        r.resp_value:league:type::string         as league_type,
        r.resp_value:league:logo::string         as league_logo,

        -- country fields
        r.resp_value:country:name::string        as country_name,
        r.resp_value:country:code::string        as country_code,
        r.resp_value:country:flag::string        as country_flag,

        -- season fields (one row per season)
        seas.value:year::number                  as season_year,
        try_to_date(seas.value:start::string)    as season_start_date,
        try_to_date(seas.value:end::string)      as season_end_date,
        seas.value:current::boolean              as is_current,

        -- keep coverage as VARIANT (you can flatten later if you want)
        seas.value:coverage                      as coverage

    from resp r,
         lateral flatten(input => r.resp_value:seasons) seas

),

dedup as (

    select
        *,
        row_number() over (
            partition by league_id, season_year
            order by ingested_at desc
        ) as rn
    from season_rows

)

select
    ingested_at,
    league_id,
    season_year as season,

    league_name,
    league_type,
    league_logo,

    country_name,
    country_code,
    country_flag,

    season_start_date,
    season_end_date,
    is_current,

    coverage
from dedup
where rn = 1


