{{ config(materialized='table') }}

-- This model is used to create a dimension table for teams. It selects distinct team IDs and their corresponding league IDs from the staging table 'stg_team_info'.
-- This model also include team IDs from the 'stg_fixture_info' table to ensure that all teams that have participated in fixtures are included in the dimension table.

with team_from_info as (
  select
    team_id:: int as team_id,
    team_name:: string as team_name,
    league_id:: int as league_id,
    ingested_at 
  from {{ ref('stg_teams_info') }}
  where team_id is not null
),

team_from_fixture as (
-- from the fixture infor table so we can union with the team info table to get all teams that have participated in fixtures
  select
    home_team_id:: int as home_team_id,
    home_team_name:: string as home_team_name,
    league_id:: int as league_id,
    ingested_at
  from {{ ref('stg_fixture_info') }}
  where home_team_id is not null

  union all

  select
    away_team_id:: int as away_team_id,
    away_team_name:: string as away_team_name,
    league_id:: int as league_id,
    ingested_at
    from {{ ref('stg_fixture_info') }}
    where away_team_id is not null
),

team_database as (
    select * from team_from_info
    union all
    select * from team_from_fixture
),

-- clean name + dedupe

cleanned_data as (
    select
    team_id,
    nullif(trim(team_name), '') as team_name,
    league_id,
    ingested_at,
    row_number() over (
        partition by team_id
        order by ingested_at desc
    ) as row_num
    from team_database
    where team_id is not null
)

select 
    team_id,
    team_name,
    league_id,
    current_timestamp as created_at,
    current_timestamp as updated_at
    
from cleanned_data
where row_num = 1

