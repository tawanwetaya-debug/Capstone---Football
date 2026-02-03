import os
import json
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import List, Dict, Any, Optional, Sequence
import requests
import snowflake.connector
from dotenv import load_dotenv
from src.extract.football_extract.extract_football import extract_team_ids, extract_team_statistics, extract_league_data, extract_team_transfer,extract_team_squad_player_ids, extract_players_statistics_byseason, extract_player_trophies_batched
from src.load.base import save_cursor,load_cursor


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
    seasons = [2018,2019,2020,2021,2022,2023,2024,2025]

    cursor = load_cursor()
    start_lg_i = cursor.get("league_i", 0)
    start_ss_i = cursor.get("season_i", 0)
    start_team_i = cursor.get("team_i", 0)
    start_page_i = cursor.get("page",1)
    stage = cursor.get("stage", "league_info")  # if missing, start at league_info

    # ========================
    # 1) extract football information
    # ========================
    try:
        for lg_i in range(start_lg_i, len(league_ids)):
            lg = league_ids[lg_i]

            for ss_i in range(start_ss_i if lg_i == start_lg_i else 0, len(seasons)):
                ss = seasons[ss_i]

                # ========================
                # 1) league information
                # ========================
                # stage = "league_info"

                if ss == 2025:
                    league_rows = extract_league_data(league_id=lg, season=ss)  # list[dict]
                    for r in league_rows:
                        insert_raw(
                            conn,
                            "FOOTBALL_CAPSTONE.RAW.RAW_LEAGUE",
                            ["ingested_at", "league_id", "season", "payload"],
                            [now_ingested(), r["league_id"], r["season"], json.dumps(r["payload"])]
                        )
                    conn.commit()
                else:
                    continue

                save_cursor(lg_i, ss_i, 0)  # done league_info (team index reset)
                print(f'beginning fetch data for league {lg}{ss}')

                # ========================
                # 2) team information (resumable)
                # ========================
                

                stage = "team_info"

                team_ids, team_rows, errors = extract_team_ids(league_id=lg, season=ss)

                if errors:
                    print(f'cannot print out team info lg{lg}ss{ss}')
                    continue 

                # figure out where to start (resume only for the starting league+season)
                start_team = start_team_i if (lg_i == start_lg_i and ss_i == start_ss_i) else 0

                # Make a lookup so we can insert team payload by team_id
                team_row_by_id = {row.get("team_id"): row for row in team_rows if row.get("payload") and row["payload"].get("team") and row["payload"]["team"].get("id") is not None
}
                buffer = []
                BATCH_SIZE = 1000

                next_team_i = start_team 
                                
                for team_i in range(start_team, len(team_ids)):
                    team_id = team_ids[team_i]

                    row = team_row_by_id.get(team_id)
                    if not row:
                        print(f"SKIP team_id={team_id} (not found in team_rows)")
                        # if API returned ids but not rows (rare), skip safely
                        next_team_i = team_i +1
                        continue

                    buffer.append([
                        now_ingested(), 
                        lg, 
                        ss, 
                        team_id, 
                        json.dumps(row["payload"])
                    ])

                    next_team_i = team_i + 1

                    if len(buffer) >= BATCH_SIZE:

                        insert_raw_many(
                            conn,
                            "FOOTBALL_CAPSTONE.RAW.RAW_TEAMS_INFO",
                            ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                            )
                        conn.commit()
                        buffer.clear()
                        
                        # ✅ persist cursor only after commit
                        save_cursor(lg_i, ss_i, next_team_i)


                if buffer:
                    insert_raw_many(
                        conn,
                        "FOOTBALL_CAPSTONE.RAW.RAW_TEAMS_INFO",
                        ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                        )
                    conn.commit()
                    buffer.clear()
                # ✅ final cursor save (even if no rows/buffer)
                save_cursor(lg_i, ss_i, next_team_i)
                print(f'beginning fetch transfer data for league {lg}{ss}')

                # ========================
                # 3) team transfers (resumable)
                # ========================
                if ss == 2025:
                    stage = "team_transfers"

                    # If you want transfers to resume from its own position,
                    # you should load start_team_i for transfers stage.
                    # If you're using one cursor for everything, this works the same way:
                    start_team = start_team_i if (lg_i == start_lg_i and ss_i == start_ss_i) else 0
      

                    for team_i in range(start_team, len(team_ids)):
                        team_id = team_ids[team_i]
                        print(f'beginning fetch data for team {team_id} in league {lg}{ss}')
                        transfer_rows = extract_team_transfer(team_id=team_id)

                        buffer = []
                        BATCH_SIZE = 1000

                        for r in transfer_rows:

                            buffer.append([
                                now_ingested(), 
                                lg, 
                                ss, 
                                team_id, 
                                json.dumps(r["payload"])                                
                            ])
                        
                        next_team_i = team_i + 1

                        if len(buffer) >= BATCH_SIZE:

                            insert_raw_many(
                                conn,
                                "FOOTBALL_CAPSTONE.RAW.RAW_TEAMS_TRANSFER",
                                ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                                # ✅ use ss directly (don’t rely on r["season"] existing)
                                
                            )
                            conn.commit()
                            buffer.clear()
                    
                    if buffer:
                        insert_raw_many(
                            conn,
                            "FOOTBALL_CAPSTONE.RAW.RAW_TEAMS_TRANSFER",
                            ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                             # ✅ use ss directly (don’t rely on r["season"] existing)
                                
                        )
                        conn.commit()
                        buffer.clear()                        
                      
   
                        # ✅ save NEXT team index
                    save_cursor(lg_i, ss_i, next_team_i)

                # ========================
                # 3) team statistics
                # ========================
                stage = "team_stats"

                print(f"beginning fetch team statistics: league={lg} season={ss}")

        #         # unpack correctly
                stats_rows, errors = extract_team_statistics(league_id=lg, season=ss)

                print("team_stats rows:", len(stats_rows), "errors:", len(errors))

                buffer = []
                BATCH_SIZE = 1000                
                
                for r in stats_rows:

                    buffer.append([
                        now_ingested(), 
                        lg, 
                        ss, 
                        team_id, 
                        json.dumps(r["payload"])                                
                        ])                    

                    if len(buffer) >= BATCH_SIZE:
                        insert_raw_many(
                            conn,
                            "FOOTBALL_CAPSTONE.RAW.RAW_TEAMS_STATISTICS",
                            ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                        )
                        conn.commit()
                        buffer.clear()  

                if buffer:      
                    insert_raw_many(
                        conn,
                        "FOOTBALL_CAPSTONE.RAW.RAW_TEAMS_STATISTICS",
                        ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                    )
                    conn.commit()
                    buffer.clear()                      
               
            # ========================
            # 4) players statisitics
            # ========================
                stage = "player_stats"

                print(f'beginning fetch player statistics for league {team_id}{lg}{ss}')

                stats_rows = extract_players_statistics_byseason(league_id=lg, season=ss, team_id=team_id)
                
                print("players_stats rows:", len(stats_rows), "errors:", len(errors))

                buffer = []
                BATCH_SIZE = 1000       

                for r in stats_rows:

                    buffer.append([
                        now_ingested(), 
                        lg, 
                        ss, 
                        r['team_id'], 
                        json.dumps(r.get("payload"))                            
                        ])   
                    


                    if len(buffer) >= BATCH_SIZE:
                        insert_raw_many(
                            conn,
                            "FOOTBALL_CAPSTONE.RAW.RAW_PLAYERS_STATSTICIS",
                            ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                        
                        )
                        conn.commit()
                        buffer.clear()
                if buffer: 
                    insert_raw_many(
                        conn,
                        "FOOTBALL_CAPSTONE.RAW.RAW_PLAYERS_STATSTICIS",
                        ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                        
                    )
                    conn.commit()
                    buffer.clear()

                    # ========================
                    # 5) team squad ids 
                    # ========================
                    # Only look for team squad ids if season is in  2025

                if ss == 2025:
                    
                        stage = 'team_squads'
                        print(f"beginning fetch team squad and trophies: league={lg} season={ss}")

                        player_ids, squad_rows, errors = extract_team_squad_player_ids(team_id=team_id)

                        if errors:
                            print("team_squads errors (first 3):", errors[:3])
                            continue


                        buffer = []
                        BATCH_SIZE = 1000 


                        for r in squad_rows:

                            buffer.append([
                                now_ingested(), 
                                lg, 
                                ss, 
                                r["team_id"], json.dumps(r["payload"])
                            ])

                            if len(buffer) >= BATCH_SIZE:

                                insert_raw_many(
                                        conn,
                                        "FOOTBALL_CAPSTONE.RAW.RAW_TEAMS_SQUADS",
                                        ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                                    )

                                conn.commit()
                                buffer.clear()  

                        if buffer:                         
                            insert_raw_many(
                                conn,
                                "FOOTBALL_CAPSTONE.RAW.RAW_TEAMS_SQUADS",
                                ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                                )

                            conn.commit()
                            buffer.clear() 


                    # ========================
                    # 6) squad trophies batch 
                    # ========================

                        stage = "player_trophies"


                        all_player_ids = []

                        for team_id in team_ids:
                            print(f'beginning fetch data for team {team_id} in league {lg}{ss}')
                            player_ids, squad_rows, errors = extract_team_squad_player_ids(team_id)
                            all_player_ids.extend(player_ids)


                        if errors:
                            save_cursor(lg_i, ss_i, team_i+1)  # หรือ team_i+1 แล้วแต่คุณอยาก skip
                            continue    

                        unique_player_ids = sorted(set(all_player_ids))

                        rows, errors = extract_player_trophies_batched(player_ids=unique_player_ids,team_id = team_id, batch_size=20)

                        print(f"[trophy] done api: rows={len(rows)} errors={len(errors) if errors else 0}")
                        print("[trophy] start snowflake insert...")
                        print("[trophy] done snowflake insert")


                        buffer = []
                        BATCH_SIZE = 1000 

                        for r in rows:

                            buffer.append([
                                now_ingested(), 
                                lg, 
                                ss,
                                team_id, 
                                json.dumps(r["payload"]) 
                            ])                            



                            if len(buffer) >= BATCH_SIZE:
                                insert_raw_many(
                                        conn,
                                        "FOOTBALL_CAPSTONE.RAW.RAW_PLAYER_TROPHIES",
                                        ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                                
                                    )
                                conn.commit()
                                buffer.clear() 

                    
                        if buffer:     
                            insert_raw_many(
                                conn,
                                "FOOTBALL_CAPSTONE.RAW.RAW_PLAYER_TROPHIES",
                                ["ingested_at", "league_id", "season", "team_id", "payload"],buffer
                                
                                )
                            conn.commit()
                            buffer.clear()   


                # after finishing seasons for this league -> reset season cursor
                save_cursor(lg_i, ss_i + 1, 0)


            # # all done -> reset cursor
            save_cursor(lg_i + 1, 0, 0)


    finally:
        conn.close()

if __name__ == "__main__":
    main()                                    
