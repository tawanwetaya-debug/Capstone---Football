from dagster import Definitions
from dagster_project.assets.dbt_assets import dbt_assets, dbt
from dagster_project.schedules import (
    live_job,
    history_job,
    live_schedule,
    history_schedule,
)

from dagster_project.jobs.load_jobs import (
    load_fixture_league_job,
    load_fixture_live_job,
    load_football_job,
    consume_fixture_live_job
)

from dagster_project.schedules import (
    daily_fixture_history,
    live_fixture_poll,
    refresh_football,
    consume_fixture_live_poll,
)

defs = Definitions(
    assets=[dbt_assets],          # dbt assets show in Catalog/Lineage
    resources={"dbt": dbt},       # dbt resource needed by dbt assets
    jobs=[
        load_fixture_league_job,
        load_fixture_live_job,
        load_football_job,
        consume_fixture_live_job,
        # optional: if you have a dbt job defined elsewhere (not required if using assets)
        live_job, 
        history_job
    ],
    schedules=[
        daily_fixture_history,
        live_fixture_poll,
        consume_fixture_live_poll,
        refresh_football,
        # optional: dbt schedules
        live_schedule, 
        history_schedule

    ],
)