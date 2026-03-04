import os
import json
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException
import snowflake.connector


TOPIC = "fixture.live.events"
GROUP_ID = "snowflake-live-events-consumer"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

SF_TABLE = "FOOTBALL_CAPSTONE.RAW.KAFKA_FIXTURE_LIVE_EVENTS"


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def build_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",   # first run reads from beginning
        "enable.auto.commit": False,       # commit only after Snowflake insert
    }
    c = Consumer(conf)
    c.subscribe([TOPIC])
    return c


def insert_batch(cur, rows):
    """
    rows: list of tuples
      (topic, partition, offset, key, value_json_str, kafka_ts_ntz_str)
    We'll parse JSON in Snowflake using PARSE_JSON.
    """
    sql = f"""
        INSERT INTO {SF_TABLE}
            (topic, partition, offset, message_key, message_value, kafka_timestamp)
        SELECT
            column1, column2, column3, column4, PARSE_JSON(column5), column6::timestamp_ntz
        FROM VALUES
    """

    # Build VALUES placeholders
    placeholders = ",".join(["(%s,%s,%s,%s,%s,%s)"] * len(rows))
    sql = sql + " " + placeholders

    # Flatten parameters
    params = []
    for r in rows:
        params.extend(list(r))

    cur.execute(sql, params)


def kafka_ts_to_ntz(ms):
    # Kafka timestamp is milliseconds since epoch
    if ms is None:
        return None
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    # store as string; Snowflake casts to TIMESTAMP_NTZ
    return dt.replace(tzinfo=None).isoformat(sep=" ")


def main():
    load_dotenv(".env.sv")  # adjust if your env file name differs

    consumer = build_consumer()
    conn = get_snowflake_connection()
    cur = conn.cursor()

    batch = []
    BATCH_SIZE = 100
    POLL_SECONDS = 1.0

    print("Consumer started. Reading Kafka and writing to Snowflake...")

    try:
        while True:
            msg = consumer.poll(POLL_SECONDS)

            if msg is None:
                # flush small batches if idle
                if batch:
                    insert_batch(cur, batch)
                    conn.commit()
                    consumer.commit(asynchronous=False)
                    print(f"Inserted {len(batch)} rows → Snowflake (idle flush)")
                    batch.clear()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()

            key = msg.key().decode("utf-8") if msg.key() else None
            value_raw = msg.value().decode("utf-8") if msg.value() else None

            # Validate JSON (optional but helpful)
            try:
                json.loads(value_raw) if value_raw else None
            except json.JSONDecodeError:
                # store as a JSON string wrapper instead of failing
                value_raw = json.dumps({"_raw": value_raw})

            kafka_ts = kafka_ts_to_ntz(msg.timestamp()[1])

            batch.append((topic, partition, offset, key, value_raw, kafka_ts))

            if len(batch) >= BATCH_SIZE:
                insert_batch(cur, batch)
                conn.commit()
                consumer.commit(asynchronous=False)  # commit offsets only after DB commit
                print(f"Inserted {len(batch)} rows → Snowflake")
                batch.clear()

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        # final flush
        if batch:
            insert_batch(cur, batch)
            conn.commit()
            consumer.commit(asynchronous=False)
            print(f"Inserted {len(batch)} rows → Snowflake (final flush)")

        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()