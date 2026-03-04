from dagster import ScheduleDefinition, AssetSelection, AssetKey, define_asset_job
from dagster import schedule

# Helper: schema-aware AssetSelection
def k(schema: str, name: str) -> AssetSelection:
    return AssetSelection.keys(AssetKey([schema, name]))

# ---- LIVE (every 2 min) ----
live_job = define_asset_job(
    name="live_fixture_job",
    selection=(
        k("STAGING", "stg_kafka_fixture_live_events")
        | k("ANALYTIC", "fct_fixture_live_event")
        | k("ANALYTIC", "fct_fixture_live_goal")
        | k("ANALYTIC", "fct_fixture_live_card")
        | k("ANALYTIC", "fct_fixture_live_substitution")
    ),
)

# ---- HISTORY (every 2 days) ----
history_job = define_asset_job(
    name="historical_fixture_job",
    selection=(
        # staging
        k("STAGING", "stg_fixture_info")
        | k("STAGING", "stg_fixture_line_up")
        | k("STAGING", "stg_fixture_players_statistics")
        | k("STAGING", "stg_fixture_statistics")
        | k("STAGING", "stg_football_manager")
        | k("STAGING", "stg_player_trophies")
        | k("STAGING", "stg_players_statistics")
        | k("STAGING", "stg_team_statistics")
        | k("STAGING", "stg_teams_info")
        | k("STAGING", "stg_league")
        # marts (dims + non-live facts)
        | k("ANALYTIC", "dim_fixture")
        | k("ANALYTIC", "dim_league")
        | k("ANALYTIC", "dim_team")
        | k("ANALYTIC", "dim_player")
        | k("ANALYTIC", "audit_manager_player_unmatched")
        | k("ANALYTIC", "fct_football_manager")
    ),
)

live_schedule = ScheduleDefinition(job=live_job, cron_schedule="*/2 * * * *")
history_schedule = ScheduleDefinition(job=history_job, cron_schedule="0 2 */2 * *")

@schedule(cron_schedule="0 2 * * *", job_name="load_fixture_league_job")
def daily_fixture_history():
    return {}

@schedule(cron_schedule="*/1 * * * *", job_name="load_fixture_live_job")
def live_fixture_poll():
    return {}

@schedule(cron_schedule="*/1 * * * *", job_name="consume_fixture_live_job")
def consume_fixture_live_poll():
    return {}


@schedule(cron_schedule="0 */3 * * *", job_name="load_football_job")
def refresh_football():
    return {}
