import os
import json
import csv
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import List, Dict, Any, Optional, Sequence
import requests
import snowflake.connector
from src.extract.football_extract.extract_fm import fetch_fm_data
from dotenv import load_dotenv

def load_env():
    load_dotenv(Path(__file__).resolve().parent / ".env")

def now_ingested():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def snowflake_conn():
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

def now_ingested_ntz_str() -> str:
    # Snowflake-friendly string for TO_TIMESTAMP_NTZ
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def insert_raw(conn, table: str, cols: List[str], values: List[Any]):
    """
    values: python list aligned with cols
    payload must already be json string if you want PARSE_JSON
    """
    # build: INSERT INTO ... (a,b,payload) SELECT TO_TIMESTAMP_NTZ(%s), %s, PARSE_JSON(%s)
    select_parts = []
    for c in cols:
        if c == "ingested_at":
            select_parts.append("TO_TIMESTAMP_NTZ(%s)")
        elif c == "payload":
            select_parts.append("PARSE_JSON(%s)")
        else:
            select_parts.append("%s")

    sql = f"""
    INSERT INTO {table} ({", ".join(cols)})
    SELECT {", ".join(select_parts)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, values)

def main():
    load_env()
    conn = snowflake_conn()

    table = "FOOTBALL_CAPSTONE.RAW.FOOTBALL_MANAGER_RAW"

    fm_folder = fetch_fm_data()          # returns WindowsPath
    fm_folder = Path(fm_folder)

    if not fm_folder.exists():
        raise FileNotFoundError(f"FM folder not found: {fm_folder}")

    inserted = 0
    try:
        # your extractor produced CSV, so scan CSV
        for file in fm_folder.glob("**/*.csv"):
            print(f"Processing {file.name}")

            extracted_at = datetime.utcnow().isoformat() + "+00:00"  # for TIMESTAMP_TZ

            with open(file, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    payload_obj = row
                    ingested_at = now_ingested_ntz_str()

                    insert_raw(
                        conn,
                        table,
                        ["ingested_at", "source_file", "payload"],
                        [ingested_at, file.name, json.dumps(payload_obj)]
                    )
                    inserted += 1

        conn.commit()
        print(f"Done. Inserted {inserted} rows.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()