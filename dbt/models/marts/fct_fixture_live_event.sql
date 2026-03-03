{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
) }}

with src as (
    select *
    from {{ ref('stg_kafka_fixture_live_events') }}
),

final as (
    select
        /* unique event id (guaranteed) */
        topic || ':' || partition || ':' || offset as event_id,

        /* kafka metadata */
        ingested_at,
        topic,
        partition,
        offset,
        message_key,
        kafka_timestamp,

        /* ordering time */
        coalesce(kafka_timestamp, ingested_at_utc, ingested_at) as event_time,

        /* business keys */
        fixture_id,

        /* event fields */
        event_elapsed_min,
        event_extra_min,
        event_type,
        event_detail,
        event_comments,

        /* entities */
        team_id,
        team_name,
        team_logo,

        player_id,
        player_name,

        assist_id,
        assist_name

    from src

    {% if is_incremental() %}
      -- only load new kafka offsets per partition/topic
      where (topic, partition, offset) not in (
        select topic, partition, offset
        from {{ this }}
      )
    {% endif %}
)

select * from final