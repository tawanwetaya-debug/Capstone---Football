import os
import json
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


