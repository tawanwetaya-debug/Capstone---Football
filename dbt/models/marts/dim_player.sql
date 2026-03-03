{{ config(materialized='table') }}

with player_info as (
    select
        ingested_at,
        player_id,
        nullif(trim(concat(coalesce(player_first_name,''), ' ', coalesce(player_last_name,''))), '') as player_name,
        player_age,
        player_nationality
    from {{ ref('stg_players_statistics') }}
    where player_id is not null
),

player_from_fixture as (
    select
        ingested_at,
        player_id,
        nullif(trim(player_name), '') as player_name,
        cast(null as number) as player_age,
        cast(null as string) as player_nationality
    from {{ ref('stg_fixture_players_statistics') }}
    where player_id is not null
),

player_database as (
    select ingested_at, player_id, player_name, player_age, player_nationality from player_info
    union all
    select ingested_at, player_id, player_name, player_age, player_nationality from player_from_fixture
),

cleaned_data as (
    select
        player_id,
        player_name,
        player_age,
        player_nationality,
        ingested_at,
        row_number() over (
            partition by player_id
            order by ingested_at desc
        ) as row_num
    from player_database
)

select
    player_id,
    player_name,
    player_age,
    player_nationality,
    current_timestamp() as created_at,
    current_timestamp() as updated_at
from cleaned_data
where row_num = 1