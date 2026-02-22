{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        try_parse_json(payload) as p
    from {{ source('football_capstone', 'raw_league') }}
),

final as (
    select
        ingested_at,
        league_id,
        season,

        -- keep raw parsed payload (optional, good for debugging)
        p as payload,

        -- league fields
        p:league:id::number       as league_id_from_payload,
        p:league:name::string     as league_name,
        p:league:type::string     as league_type,
        p:league:logo::string     as league_logo,

        -- country fields
        p:country:name::string    as country_name,
        p:country:code::string    as country_code,
        p:country:flag::string    as country_flag

    from src
)

select * from final
