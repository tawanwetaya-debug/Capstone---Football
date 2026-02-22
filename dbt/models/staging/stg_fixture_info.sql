{{ config(materialized='view', enabled=true) }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        fixture_id,
        payload as resp_value
    from {{ source('football_capstone', 'RAW_FIXTURE_INFO') }}
    where payload is not null
)

select
    ingested_at,
    league_id,
    season,
    fixture_id,

    -- fixture
    resp_value:fixture:id::number                                as api_fixture_id,
    resp_value:fixture:date::timestamp_tz                         as fixture_date_utc,
    resp_value:fixture:timestamp::number                          as fixture_timestamp,
    resp_value:fixture:timezone::string                           as fixture_timezone,
    resp_value:fixture:referee::string                            as referee,

    -- periods
    resp_value:fixture:periods:first::number                      as period_first_ts,
    resp_value:fixture:periods:second::number                     as period_second_ts,

    -- status
    resp_value:fixture:status:elapsed::number                     as status_elapsed,
    resp_value:fixture:status:extra::number                       as status_extra,
    resp_value:fixture:status:long::string                        as status_long,
    resp_value:fixture:status:short::string                       as status_short,

    -- venue
    resp_value:fixture:venue:id::number                           as venue_id,
    resp_value:fixture:venue:name::string                         as venue_name,
    resp_value:fixture:venue:city::string                         as venue_city,

    -- goals
    resp_value:goals:home::number                                 as goals_home,
    resp_value:goals:away::number                                 as goals_away,

    -- league
    resp_value:league:id::number                                  as api_league_id,
    resp_value:league:name::string                                as league_name,
    resp_value:league:country::string                             as league_country,
    resp_value:league:season::number                              as league_season,
    resp_value:league:round::string                               as league_round,
    resp_value:league:standings::boolean                          as league_standings,
    resp_value:league:logo::string                                as league_logo_url,
    resp_value:league:flag::string                                as league_flag_url,

    -- teams
    resp_value:teams:home:id::number                              as home_team_id,
    resp_value:teams:home:name::string                            as home_team_name,
    resp_value:teams:home:logo::string                            as home_team_logo_url,
    resp_value:teams:home:winner::boolean                         as home_team_winner,

    resp_value:teams:away:id::number                              as away_team_id,
    resp_value:teams:away:name::string                            as away_team_name,
    resp_value:teams:away:logo::string                            as away_team_logo_url,
    resp_value:teams:away:winner::boolean                         as away_team_winner,

    -- score
    resp_value:score:halftime:home::number                        as score_ht_home,
    resp_value:score:halftime:away::number                        as score_ht_away,

    resp_value:score:fulltime:home::number                        as score_ft_home,
    resp_value:score:fulltime:away::number                        as score_ft_away,

    resp_value:score:extratime:home::number                       as score_et_home,
    resp_value:score:extratime:away::number                       as score_et_away,

    resp_value:score:penalty:home::number                         as score_p_home,
    resp_value:score:penalty:away::number                         as score_p_away,

    -- debug
    resp_value                                                     as payload_response

from src