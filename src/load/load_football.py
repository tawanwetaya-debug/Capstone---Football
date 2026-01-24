import os
import json
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import List, Dict, Any, Optional
import requests
import snowflake.connector
from dotenv import load_dotenv
from src.extract.football_extract.extract_football import extract_team_ids, extract_team_statistics, extract_league_data, extract_team_transfer,extract_team_squad_player_ids
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


def main():
    load_env()
    conn = snowflake_conn()

    with conn.cursor() as cur:
        cur.execute("select current_account(), current_user(), current_role(), current_database(), current_schema()")
        print("SESSION =", cur.fetchone())

    # League_ID 39 = Premier League, 140 = La Liga, 135 = Serie A, 78 = Bundesliga, 61 = Ligue 1
    league_ids = [39,140,135,78,61]  # Premier League, La Liga   
    seasons = [2018,2019,2020,2021,2022,2023,2024]

    cursor = load_cursor()
    start_lg_i = cursor.get("league_i", 0)
    start_ss_i = cursor.get("season_i", 0)
    start_team_i = cursor.get("team_i", 0)
    start_page_i = cursor.get("page",1)
    stage = cursor.get("stage", "league_info")  # if missing, start at league_info

    try:
        for lg_i in range(start_lg_i, len(league_ids)):
            lg = league_ids[lg_i]

            for ss_i in range(start_ss_i if lg_i == start_lg_i else 0, len(seasons)):
                ss = seasons[ss_i]

                # ========================
                # 1) league information
                # ========================
                stage = "league_info"

                league_rows = extract_league_data(league_id=lg, season=ss)  # list[dict]
                for r in league_rows:
                    insert_raw(
                        conn,
                        "FOOTBALL.RAW.RAW_LEAGUE",
                        ["ingested_at", "league_id", "season", "payload"],
                        [now_ingested(), r["league_id"], r["season"], json.dumps(r["payload"])]
                    )
                conn.commit()

                save_cursor(lg_i, ss_i, 0)  # done league_info (team index reset)

                # ========================
                # 2) team information (resumable)
                # ========================
                stage = "team_info"

                team_ids, team_rows, errors = extract_team_ids(league_id=lg, season=ss)

                # figure out where to start (resume only for the starting league+season)
                start_team = start_team_i if (lg_i == start_lg_i and ss_i == start_ss_i) else 0

                # Make a lookup so we can insert team payload by team_id
                team_row_by_id = {row["team_id"]: row for row in team_rows}

                for team_i in range(start_team, len(team_ids)):
                    team_id = team_ids[team_i]

                    row = team_row_by_id.get(team_id)
                    if not row:
                        # if API returned ids but not rows (rare), skip safely
                        continue

                    insert_raw(
                        conn,
                        "FOOTBALL.RAW.RAW_TEAMS_INFO",
                        ["ingested_at", "league_id", "season", "team_id", "payload"],
                        [now_ingested(), lg, ss, team_id, json.dumps(row["payload"])]
                    )
                    conn.commit()

                    # ✅ save NEXT team index (resume point)
                    save_cursor(lg_i, ss_i, team_i + 1)

                # optional: after finishing team_info fully for this season, reset team cursor
                save_cursor(lg_i, ss_i, 0)


                # ========================
                # 3) team transfers (resumable)
                # ========================
                stage = "team_transfers"

                # If you want transfers to resume from its own position,
                # you should load start_team_i for transfers stage.
                # If you're using one cursor for everything, this works the same way:
                start_team = start_team_i if (lg_i == start_lg_i and ss_i == start_ss_i) else 0

                for team_i in range(start_team, len(team_ids)):
                    team_id = team_ids[team_i]

                    transfer_rows, all_errors = extract_team_transfer(team_id=team_id)

                    for r in transfer_rows:
                        insert_raw(
                            conn,
                            "FOOTBALL.RAW.RAW_TEAMS_TRANSFER",
                            ["ingested_at", "league_id", "season", "team_id", "payload"],
                            # ✅ use ss directly (don’t rely on r["season"] existing)
                            [now_ingested(), lg, ss, team_id, json.dumps(r["payload"])]
                        )

                    conn.commit()

                    # ✅ save NEXT team index
                    save_cursor(lg_i, ss_i, team_i + 1)

                # optional: after finishing transfers fully for this season, reset team cursor
                save_cursor(lg_i, ss_i, 0)

                # ========================
                # 3) team statistics
                # ========================
                stage = "team_stats"

                stats_rows, all_errors = extract_team_statistics(league_id=lg, season=ss)
                for r in stats_rows:
                    insert_raw(
                        conn,
                        "FOOTBALL.RAW.RAW_TEAMS_STATSTICIS",
                        ["ingested_at", "league_id", "season", "team_id", "payload"],
                        [now_ingested(), r["league_id"], r["season"], r["team_id"], json.dumps(r["payload"])]
                    )
                conn.commit()

                save_cursor(lg_i, ss_i, 0)  # done team_stats (team index reset)
                
              