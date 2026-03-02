import json
from dotenv import load_dotenv
import os
import time
import requests
import snowflake.connector
from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Producer

# CONFIG

BASE_URL = "https://v3.football.api-sports.io"
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fixture.live.events"
POLL_INTERVAL = 20

CURSOR_PATH = Path("state/live_events_cursor.json")


def get_snowflake_connection():
    load_dotenv("env.sv")
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

def get_active_fixtures():
    conn = get_snowflake_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT fixture_id
        FROM (
            SELECT 
                fixture_id,
                ROW_NUMBER() OVER (
                    PARTITION BY fixture_id
                    ORDER BY ingested_at DESC
                ) AS rn
            FROM FOOTBALL_CAPSTONE.RAW_STAGING.STG_FIXTURE_INFO
            WHERE season = 2025
            AND fixture_date_utc >= DATEADD(day, -3, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
            AND fixture_date_utc <= CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        )
        WHERE rn = 1
    """)

    fixtures = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    return fixtures


def load_cursor():
    if CURSOR_PATH.exists():
        return json.loads(CURSOR_PATH.read_text())
    return {}


def save_cursor(cursor):
    CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    CURSOR_PATH.write_text(json.dumps(cursor, indent=2))


def get_fixture_events(fixture_id):
    API_KEY = "dd82933e09e4052c68aafecf1feb4634"
    headers = {"x-apisports-key": API_KEY}
    r = requests.get(
        f"{BASE_URL}/fixtures",
        headers=headers,
        params={"id": int(fixture_id)},
        timeout=30
    )
    r.raise_for_status()
    return r.json().get("response", [])


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    cursor = load_cursor()

    
    while True:
        try:
            fixtures = get_active_fixtures()

            for fixture_id in fixtures:
                events = get_fixture_events(fixture_id)
                print(f'{fixture_id} is successful access')

                last_seen = cursor.get(str(fixture_id), 0)

                for event in events:
                    minute = event.get("time", {}).get("elapsed")

                    if minute and minute > last_seen:
                        payload = {
                            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
                            "fixture_id": fixture_id,
                            "event": event
                        }

                        print("Producing →", json.dumps(payload, indent=2))
                        producer.produce(
                            topic=TOPIC,
                            key=str(fixture_id),
                            value=json.dumps(payload)
                        )

                        producer.poll(0)

                        cursor[str(fixture_id)] = minute

            save_cursor(cursor)
            producer.flush()

        except Exception as e:
            print("ERROR:", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()