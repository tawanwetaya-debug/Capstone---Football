from dagster import op, job
from dagster_project.jobs import __init__
from src.load.load_football import main as load_football_run
from src.kafka.produce_live_events import main as load_fixture_live_run
from src.kafka.consume_live_events import main as consume_fixture_live_run
from src.load.load_fixture_history import main as load_fixture_league_run



@op
def load_fixture_league_op():
    load_fixture_league_run()


@op
def load_fixture_live_op():
    load_fixture_live_run()


@op
def load_football_op():
    load_football_run()

@op
def consume_fixture_live_op():
    consume_fixture_live_run()

@job
def load_fixture_league_job():
    load_fixture_league_op()


@job
def load_fixture_live_job():
    load_fixture_live_op()


@job
def load_football_job():
    load_football_op()

@job
def consume_fixture_live_job():
    consume_fixture_live_op()