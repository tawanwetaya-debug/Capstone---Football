from dagster import Definitions
from dagster_project.assets.dbt_assets import dbt_assets, dbt
from dagster_project.schedules import (
    live_job,
    history_job,
    live_schedule,
    history_schedule,
)

defs = Definitions(
    assets=[dbt_assets],  # + any other assets you have
    resources={"dbt": dbt},
    jobs=[live_job, history_job],   # 👈 IMPORTANT
    schedules=[live_schedule, history_schedule],  # 👈 IMPORTANT
)