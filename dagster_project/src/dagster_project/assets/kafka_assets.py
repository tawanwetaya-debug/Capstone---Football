from dagster import asset
import subprocess

@asset
def produce_live_events():
    subprocess.run(["python", "-m", "src.kafka.produce_live_events"], check=True)

@asset(deps=[produce_live_events])
def consume_live_events():
    subprocess.run(["python", "-m", "src.kafka.consume_live_events"], check=True)