{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        payload as p
    from {{ source('football_capstone', 'FOOTBALL_MANAGER_RAW') }}
)

select
    ingested_at,
    p:"﻿Name"::string as player_name,
    p:Nationality::string as nationality,
    p as payload_json

from src
where p is not null

