{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        fixture_id,
        payload
    from {{ source('football_capstone', 'RAW_FIXTURE_STATISTICS') }}
),

-- explode statistics[] to rows first
stats as (
    select
        s.ingested_at,
        s.league_id,
        s.season,
        s.fixture_id,

        s.payload:team:id::number      as team_id,
        s.payload:team:name::string    as team_name,
        s.payload:team:logo::string    as team_logo_url,

        st.value:type::string          as stat_type,
        st.value:value                 as stat_value_raw
    from src s,
         lateral flatten(input => s.payload:statistics) st
),

final as (
    select
        ingested_at,
        league_id,
        season,
        fixture_id,
        team_id,
        team_name,
        team_logo_url,

        -- numeric stats
        max(case when stat_type = 'Shots on Goal'      then try_to_number(stat_value_raw::string) end) as shots_on_goal,
        max(case when stat_type = 'Shots off Goal'     then try_to_number(stat_value_raw::string) end) as shots_off_goal,
        max(case when stat_type = 'Total Shots'        then try_to_number(stat_value_raw::string) end) as total_shots,
        max(case when stat_type = 'Blocked Shots'      then try_to_number(stat_value_raw::string) end) as blocked_shots,
        max(case when stat_type = 'Shots insidebox'    then try_to_number(stat_value_raw::string) end) as shots_inside_box,
        max(case when stat_type = 'Shots outsidebox'   then try_to_number(stat_value_raw::string) end) as shots_outside_box,
        max(case when stat_type = 'Fouls'              then try_to_number(stat_value_raw::string) end) as fouls,
        max(case when stat_type = 'Corner Kicks'       then try_to_number(stat_value_raw::string) end) as corner_kicks,
        max(case when stat_type = 'Offsides'           then try_to_number(stat_value_raw::string) end) as offsides,
        max(case when stat_type = 'Yellow Cards'       then try_to_number(stat_value_raw::string) end) as yellow_cards,
        max(case when stat_type = 'Red Cards'          then try_to_number(stat_value_raw::string) end) as red_cards,
        max(case when stat_type = 'Goalkeeper Saves'   then try_to_number(stat_value_raw::string) end) as goalkeeper_saves,
        max(case when stat_type = 'Total passes'       then try_to_number(stat_value_raw::string) end) as total_passes,
        max(case when stat_type = 'Passes accurate'    then try_to_number(stat_value_raw::string) end) as passes_accurate,

        -- percentage stats (strip %)
        max(case when stat_type = 'Ball Possession'
                 then try_to_number(replace(stat_value_raw::string, '%','')) end) as ball_possession_pct,

        max(case when stat_type = 'Passes %'
                 then try_to_number(replace(stat_value_raw::string, '%','')) end) as passes_pct
                 

    from stats
    group by
        ingested_at,
        league_id,
        season,
        fixture_id,
        team_id,
        team_name,
        team_logo_url
)

select * from final