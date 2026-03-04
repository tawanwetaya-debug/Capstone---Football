{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        topic,
        partition,
        offset,
        message_key,
        message_value,
        kafka_timestamp
    from {{ source('football_capstone', 'KAFKA_FIXTURE_LIVE_EVENTS') }}
),

parsed as (
    select
        ingested_at,
        topic,
        partition,
        offset,
        message_key,
        kafka_timestamp,

        /* message_value is usually VARIANT already (because you used PARSE_JSON on insert),
           but this makes it safe if it ever becomes string */
        case
            when is_object(message_value) then message_value
            else try_parse_json(message_value)
        end as p
    from src
),

final as (
    select
        ingested_at,
        topic,
        partition,
        offset,
        message_key,
        kafka_timestamp,
        p as message_value_json,

        /* top-level fields you embedded in producer */
        p:fixture_id::number                                  as fixture_id,
        p:ingested_at_utc::timestamp_ntz                      as ingested_at_utc,

        /* event core */
        p:event:time:elapsed::number                          as event_elapsed_min,
        p:event:time:extra::number                            as event_extra_min,
        p:event:type::string                                  as event_type,
        p:event:detail::string                                as event_detail,
        p:event:comments::string                              as event_comments,

        /* team */
        p:event:team:id::number                               as team_id,
        p:event:team:name::string                             as team_name,
        p:event:team:logo::string                             as team_logo,

        /* player */
        p:event:player:id::number                             as player_id,
        p:event:player:name::string                           as player_name,

        /* assist */
        p:event:assist:id::number                             as assist_id,
        p:event:assist:name::string                           as assist_name

    from parsed
    where p is not null
)

select * from final