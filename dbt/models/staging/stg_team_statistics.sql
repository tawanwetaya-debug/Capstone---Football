{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        league_id,
        season,
        payload
    from {{ source('football_capstone', 'RAW_TEAMS_STATISTICS') }}
),

final as (
    select
        ingested_at,
        league_id,
        season,
        p as payload_json,

        -- parse payload string -> VARIANT object
        p:team:id::number        as team_id,
        p:team:name::string      as team_name,
        p:team:logo::string      as team_logo_url,

        p:form::string           as form,

        p:fixtures:played:total::number  as fixtures_played_total,
        p:fixtures:wins:total::number    as fixtures_wins_total,
        p:fixtures:draws:total::number   as fixtures_draws_total,
        p:fixtures:loses:total::number   as fixtures_loses_total,

        p:goals:for:total:total::number      as goals_for_total,
        p:goals:against:total:total::number  as goals_against_total,

        try_to_number(replace(p:goals:for:average:total::string, '%',''))     as goals_for_avg_total,
        try_to_number(replace(p:goals:against:average:total::string, '%','')) as goals_against_avg_total,

        p:clean_sheet:total::number     as clean_sheet_total,
        p:failed_to_score:total::number as failed_to_score_total,

        p:penalty:scored:total::number  as penalty_scored_total,
        p:penalty:missed:total::number  as penalty_missed_total

    from src,
    lateral (select try_parse_json(src.payload) as p)
)

select *
from final