import os
import json
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import List, Dict, Any, Optional
import requests
import snowflake.connector
from dotenv import load_dotenv
from src.extract.football_extract.extract_fixture import extract_league_fixture, extract_fixture_events, extract_fixture_lineups, extract_fixture_statistic, extract_fixture_predictions,extract_fixture_odds, extract_fixture_players_statistic
from src.load.base import save_cursor,load_cursor


def load_env():
    load_dotenv(Path(__file__).resolve().parent / ".env")

def now_ingested():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def snowflake_conn():
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

def insert_many(conn, table, columns, rows):
    if not rows:
        return

    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    sql = f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
    """

    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
    finally:
        cur.close()


def insert_raw_many(conn, table: str, cols: List[str], rows: Sequence[Sequence[Any]]):
    """
    rows: list of value-lists aligned with cols
    - ingested_at is cast to TIMESTAMP_NTZ
    - payload is parsed to VARIANT
    """
    if not rows:
        return

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
        cur.executemany(sql, rows) 


def main():
    load_env()
    conn = snowflake_conn()

    with conn.cursor() as cur:
        cur.execute("select current_account(), current_user(), current_role(), current_database(), current_schema()")
        print("SESSION =", cur.fetchone())

    # League_ID 39 = Premier League, 140 = La Liga, 135 = Serie A, 78 = Bundesliga, 61 = Ligue 1
    league_ids = [39,140,135,78,61]  # Premier League, La Liga   
    seasons = [2018,2019,2020,2021,2022,2023,2024,2025]

    cursor = load_cursor()
    start_lg_i = cursor.get("league_i", 0)
    start_ss_i = cursor.get("season_i", 0)
    start_team_i = cursor.get("team_i", 0)
    start_fixture_i = cursor.get('fixture_i',0)
    start_page_i = cursor.get("page",1)
    stage = cursor.get("stage", "fixture_info")  # if missing, start at league_info
   
    # ========================
    # 1) extract fixture information
    # ========================
    try:
        for lg_i in range(start_lg_i, len(league_ids)):
            lg = league_ids[lg_i]

            for ss_i in range(start_ss_i if lg_i == start_lg_i else 0, len(seasons)):
                ss = seasons[ss_i]

                # ========================
                # 1) fixture information
                # ========================
                stage = "fixture_info"

                fixture_ids, fixture_rows, errors = extract_league_fixture(league_id=lg,season=ss)

                # figure out where to start (fixture id)
                start_fixture = start_fixture_i if (lg_i == start_lg_i and ss_i == start_ss_i) else 0

                # Make a lookup so we can insert fixture payload by fixture_id
                fixture_row_by_id = {row['fixture_id']: row for row in fixture_rows}

                buffer = []
                BATCH_SIZE = 1000
       
                for fixture_i in range(start_fixture, len(fixture_ids)):
                    fixture_id = fixture_ids[fixture_i]

                    row = fixture_row_by_id.get(fixture_id)
                    if not row:
                        continue

                    # build ONE row (values aligned with columns)
                    buffer.append([
                        now_ingested(),                 # ingested_at
                        lg,                             # league_id
                        ss,                             # season
                        fixture_id,                     # fixture_id
                        row["home_team_id"],            # home_team_id
                        row["away_team_id"],            # away_team_id
                        json.dumps(row["payload"]),     # payload (string for PARSE_JSON)
                    ])

                    # flush batch
                    if len(buffer) >= BATCH_SIZE:
                        insert_raw_many(
                            conn,
                            "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_INFO",
                            ["ingested_at", "league_id", "season", "fixture_id", "home_team_id", "away_team_id", "payload"],
                            buffer
                        )
                        conn.commit()
                        buffer.clear()

                        # ✅ save NEXT fixture index only after commit succeeds
                        save_cursor(lg_i, ss_i, fixture_i + 1)

                # flush remaining rows at end
                if buffer:
                    insert_raw_many(
                        conn,
                        "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_INFO",
                        ["ingested_at", "league_id", "season", "fixture_id", "home_team_id", "away_team_id", "payload"],
                        buffer
                    )
                    conn.commit()
                    buffer.clear()

                    save_cursor(lg_i, ss_i, len(fixture_ids))  # fully done for this league+season

                
                # ========================
                # 2) fixture event
                # ========================
                stage = "fixture_event"                    

                buffer = []
                BATCH_SIZE = 1000

                event_rows, errors = extract_fixture_events(fixture_id=fixture_id)

                for r in event_rows:

                    buffer.append([
                        now_ingested(), 
                        lg, 
                        ss, 
                        fixture_id, 
                        json.dumps(row["payload"])
                    ])

                    if len(buffer) >= BATCH_SIZE:
                        insert_raw_many(
                            conn,
                            "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_EVENT",
                            ["ingested_at", "league_id", "season", "fixture_id", "payload"],
                            buffer
                        )
                        conn.commit()
                        buffer.clear()
                
                # ✅ save NEXT fixture index (resume point)
                save_cursor(lg_i, ss_i, fixture_i + 1)

                if buffer:
                    insert_raw_many(
                        conn,
                        "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_EVENT",
                        ["ingested_at", "league_id", "season", "fixture_id", "payload"],
                        buffer
                    )
                    conn.commit()
                    buffer.clear()                    

                # optional: after finishing fixture_info fully for this season, reset fixture cursor
                save_cursor(lg_i, ss_i, len(fixture_ids))

                # ========================
                # 3) fixture line up
                # ========================
                stage = "fixture_line_up"

                buffer = []
                BATCH_SIZE = 1000

                lineup_rows, errors = extract_fixture_lineups(fixture_id=fixture_id)

                for row in lineup_rows:


                    buffer.append([
                            now_ingested(), 
                            lg, 
                            ss, 
                            fixture_id, 
                            row['team_id'],
                            json.dumps(row["payload"])

                    ])


                    if len(buffer) >= BATCH_SIZE:

                        insert_raw_many(
                                conn,
                                "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_LINE_UP",
                                ["ingested_at", "league_id", "season", "fixture_id","team_id", "payload"],
                                buffer
                            )
                        conn.commit()
                        buffer.clear()   

                if buffer:
                    insert_raw_many(
                            conn,
                            "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_LINE_UP",
                            ["ingested_at", "league_id", "season", "fixture_id","team_id", "payload"],
                            buffer
                        )
                    conn.commit()
                    buffer.clear()


                # ✅ save NEXT fixture index (resume point)
                save_cursor(lg_i, ss_i, fixture_i + 1)

                # optional: after finishing fixture_info fully for this season, reset fixture cursor
                save_cursor(lg_i, ss_i, 0)

                # ========================
                # 4) fixture team statistics 
                # ========================

                stage = "fixture_team_statistics"

                fixture_rows, errors = extract_fixture_statistic(fixture_id=fixture_id)

                buffer = []
                BATCH_SIZE = 1000

                buffer.append([
                    now_ingested(), 
                    lg, 
                    ss, 
                    fixture_id, 
                    json.dumps(row["payload"])
                   
                ])

                insert_raw(
                        conn,
                        "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_STATISTICS",
                        ["ingested_at", "league_id", "season", "fixture_id","team_id", "payload"],
                    )
                
                conn.commit()
                buffer.clear()   

                # ✅ save NEXT fixture index (resume point)
                save_cursor(lg_i, ss_i, fixture_i + 1)

                # optional: after finishing fixture_info fully for this season, reset fixture cursor
                save_cursor(lg_i, ss_i, 0)                
                
                # ========================
                # 5) fixture match prediction
                # ========================

                stage = "fixture_team_prediction"

                fixture_rows, errors = extract_fixture_predictions(fixture_id=fixture_id)

                buffer = []
                BATCH_SIZE = 1000                


                insert_raw(
                        conn,
                        "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_PREDICTIONS",
                        ["ingested_at", "league_id", "season", "fixture_id","team_id", "payload"],
                        [now_ingested(), lg, ss, fixture_id, json.dumps(row["payload"])]
                    )
                conn.commit()

                # ✅ save NEXT fixture index (resume point)
                save_cursor(lg_i, ss_i, fixture_i + 1)

                # optional: after finishing fixture_info fully for this season, reset fixture cursor
                save_cursor(lg_i, ss_i, 0)   

                # ========================
                # 6) fixture odds
                # ========================

                stage = "fixture_team_prediction"

                fixture_rows, errors = extract_fixture_odds(fixture_id=fixture_id)

                insert_raw(
                        conn,
                        "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_ODDS",
                        ["ingested_at", "league_id", "season", "fixture_id","team_id", "payload"],
                        [now_ingested(), lg, ss, fixture_id, json.dumps(row["payload"])]
                    )
                conn.commit()

                # ✅ save NEXT fixture index (resume point)
                save_cursor(lg_i, ss_i, fixture_i + 1)

                # optional: after finishing fixture_info fully for this season, reset fixture cursor
                save_cursor(lg_i, ss_i, 0)   


                # ========================
                # 6) fixture player statistics
                # ========================

                stage = "fixture_player_statistics"

                fixture_row = fixture_row_by_id.get(fixture_id)

                team_ids = [
                    fixture_row.get("home_team_id"),
                    fixture_row.get("away_team_id"),
                ]

                for idx, team_id in enumerate (team_ids):
                    if not team_id:
                        continue

                    side = 'home' if idx == 0 else "away"

                    player_rows, player_errors = extract_fixture_players_statistic(fixture_id=fixture_id,team_id=team_id,side = side)
                        
                    for player_row in player_rows:

                        insert_raw(
                                conn,
                                "FOOTBALL_CAPSTONE.RAW.RAW_FIXTURE_PLAYERS_STATISTIC",
                                ["ingested_at", "league_id", "season", "fixture_id","team_id", "payload"],
                                [now_ingested(), lg, ss, fixture_id, json.dumps(row["payload"])]
                            )
                        conn.commit()

                    # ✅ save NEXT fixture index (resume point)
                    save_cursor(lg_i, ss_i, fixture_i + 1)

                    # optional: after finishing fixture_info fully for this season, reset fixture cursor
                    save_cursor(lg_i, ss_i, 0)   
                
    finally:
        conn.close()

if __name__ == "__main__":
    main()                