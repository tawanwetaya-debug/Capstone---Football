from src.extract.football_api.api_fixture import fetch_fixture_data,fetch_fixture_events,fetch_fixture_lineups,fetch_match_odd,fetch_match_prediction,fetch_players_statistic, fetch_fixture_statistic
from typing import Optional, Tuple, List, Dict, Any
import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

def export_json(data: dict, prefix: str):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    file_path = out_dir / f"{prefix}_{ts}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]



def extract_league_fixture(
    season: int,
    league_id: int,
    date: Optional[str] = None,
    limit: Optional[int] = None
) -> Tuple[List[int], List[Dict[str, Any]], List[Any]]:

    fixture_data = fetch_fixture_data(league_id=league_id, season=season, date=date)

    data = fixture_data.get("response", []) or []
    errors = fixture_data.get("errors", []) or fixture_data.get("error", []) or []

    if limit is not None:
        data = data[:limit]

    fixture_ids: List[int] = []
    fixture_rows: List[Dict[str, Any]] = []

    for item in data:
        fixture = item.get("fixture", {}) or {}
        teams = item.get("teams", {}) or {}
        home = teams.get("home", {}) or {}
        away = teams.get("away", {}) or {}

        fixture_id = fixture.get("id")
        if not fixture_id:
            continue

        fixture_ids.append(fixture_id)

        fixture_rows.append({
            "league_id": league_id,
            "season": season,
            "fixture_id": fixture_id,
            "fixture_date": fixture.get("date"),
            "home_team_id": home.get("id"),
            "away_team_id": away.get("id"),
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload": item
        })

    return fixture_ids, fixture_rows, errors
        

def extract_fixture_events(
    fixture_id: int,
    limit: Optional[int] = None
) -> Tuple[List[Dict[str, Any]], List[Any]]:

    fixture_events = fetch_fixture_events(fixture_id=fixture_id)

    data = fixture_events.get("response", []) or []
    errors = fixture_events.get("errors", []) or fixture_events.get("error", []) or []

    if limit is not None:
        data = data[:limit]

    event_rows: List[Dict[str, Any]] = []

    for item in data:
        event_rows.append({
            "fixture_id": fixture_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload": item
        })

    return event_rows, errors


def extract_fixture_lineups(fixture_id: int, limit: Optional[int] = None) -> Tuple[List[Dict[str, Any]],List[Any]]:

    fixture_lineup = fetch_fixture_lineups(fixture_id=fixture_id)

    data= fixture_lineup.get("response",[]) or []
    errors= fixture_lineup.get("errors", []) or fixture_lineup.get("error", []) or []


    if limit is not None:
        data = data[:limit]
    
    lineup_rows: List[Dict[str, Any]] = []

    for idx, item in enumerate(data):
        team = item.get("team", {}) or {}

        side = "home" if idx == 0 else "away" if idx == 1 else f"unknown_{idx}"

        lineup_rows.append({
            "fixture_id": fixture_id,
            "side": side,  # home/away
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload": item
        })

    return lineup_rows, errors

def extract_fixture_statistic(fixture_id: int, team_id: int, limit: Optional[int] = None) -> Tuple[List[Dict[str, Any]],List[Any]]:

    fixture_statistic = fetch_fixture_statistic(fixture_id=fixture_id, team_id=team_id)
    data = fixture_statistic.get("response",[]) or []
    errors = fixture_statistic.get("errors", []) or fixture_statistic.get("error", []) or []


    if limit is not None:
        data = data[:limit]
          
    raw_rows = []
    for item in data:
        raw_rows.append({
            'fixture_id' :fixture_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload" : item

        }

    )    
    
    return raw_rows, errors

def extract_fixture_players_statistic(fixture_id: int, team_id: int, side: str, limit: Optional[int] = None) -> Tuple[List[Dict[str, Any]],List[Any]]:

    fixture_players_statistic = fetch_players_statistic(fixture_id=fixture_id, team_id=team_id)
    data = fixture_players_statistic.get("response",[]) or []
    errors = fixture_players_statistic.get("errors", []) or fixture_players_statistic.get("error", []) or []

    if limit is not None:
        data = data[:limit]
          
    raw_rows = []
    for item in data:
        raw_rows.append({
            'fixture_id' :fixture_id,
            'team_id' : team_id,
            "side": side,  # home/away
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload" : item

        }

    )        
    return raw_rows , errors    


def extract_fixture_predictions(fixture_id: int,limit: Optional[int] = None) -> Tuple[List[Dict[str, Any]],List[Any]]:

    fixture_predictions = fetch_match_prediction(fixture_id=fixture_id)
    data = fixture_predictions.get("response",[]) or []
    errors = fixture_predictions.get("errors", []) or fixture_predictions.get("error", []) or []


    if limit is not None:
        data = data[:limit]
          
    raw_rows = []
    for item in data:
        raw_rows.append({
            'fixture_id' :fixture_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload" : item

        }

    )    
    
    return raw_rows, errors


def extract_fixture_odds(fixture_id: int,limit: Optional[int] = None) -> Tuple[List[Dict[str, Any]],List[Any]]:

    fixture_odds = fetch_match_odd(fixture_id=fixture_id)
    data = fixture_odds.get("response",[]) or []
    errors = fixture_odds.get("errors", []) or fixture_odds.get("error", []) or []


    if limit is not None:
        data = data[:limit]
          
    raw_rows = []
    for item in data:
        raw_rows.append({
            'fixture_id' :fixture_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload" : item

        }

    )    
    
    return raw_rows, errors


LIVE_CODES = {"1H", "2H", "HT", "ET", "BT", "P", "INT"}

def extract_league_fixture_live_today(
    league_id: int,
    season: int,
    limit: int | None = None,
):
    today = datetime.now(ZoneInfo("Asia/Bangkok")).date().isoformat()

    fixture_ids, rows, errors = extract_league_fixture(
        league_id=league_id,
        season=season,
        date=today,
        limit=limit
    )

    # filter only live rows
    live_rows = []
    live_ids = []

    for r in rows:
        status = (r.get("payload", {}) or {}).get("fixture", {}).get("status", {}) or {}
        code = status.get("short")
        if code in LIVE_CODES:
            r["feed_type"] = "live"
            live_rows.append(r)
            live_ids.append(r["fixture_id"])

    return live_ids, live_rows, errors


if __name__ == "__main__":
    league_id = 39  # Premier League
    season = 2025
    limit = 3

    # 1) extract live fixtures (today + status filter)
    live_ids, live_rows, errors = extract_league_fixture_live_today(
        league_id=league_id,
        season=season,
        limit=limit
    )

    export_json(
        data=live_rows,
        prefix=f"league_fixture_live_l{league_id}_s{season}"
    )
    if errors:
        print("fixture live errors:", errors)

    if not live_ids:
        print("No live fixtures right now.")

    # 2) extract events for each live fixture
    for fixture_id in live_ids:
        event_rows, ev_errors = extract_fixture_events(fixture_id=fixture_id)

        export_json(
            data=event_rows,
            prefix=f"league_fixture_live_events_f{fixture_id}_l{league_id}_s{season}"
        )

        if ev_errors:
            print(f"fixture {fixture_id} event errors:", ev_errors)


        raw_rows, ev_errors = extract_fixture_odds(fixture_id=fixture_id)

        export_json(
            data=raw_rows,
            prefix=f"fixture_live_odds_f{fixture_id}_l{league_id}_s{season}"
        )

        if ev_errors:
            print(f"fixture {raw_rows} event errors:", ev_errors)


    # 1) extract fixture_id(s)
    fixture_ids, fixture_rows, errors = extract_league_fixture(
        league_id=league_id,
        season=season,
        limit=limit
    )

    export_json(
        data=fixture_rows,
        prefix=f"league_fixture_l{league_id}_s{season}"
    )
    if errors:
        print("fixture list errors:", errors)
    
    fixture_map = {r.get("fixture_id"): r for r in fixture_rows}

    # for fixture_id in fixture_ids:
        # # 2) events
        # event_rows, event_errors = extract_fixture_events(fixture_id=fixture_id)
        # print(f"extract fixture events {fixture_id}")

        # export_json(
        #     data=event_rows,
        #     prefix=f"fixture_events_l{league_id}_s{season}_f{fixture_id}"
        # )
        # if event_errors:
        #     print("event errors:", event_errors)

        # # 3) lineups
        # lineup_rows, lineup_errors = extract_fixture_lineups(fixture_id=fixture_id)
        # print(f"extract fixture lineups {fixture_id}")

        # export_json(
        #     data=lineup_rows,
        #     prefix=f"fixture_lineups_l{league_id}_s{season}_f{fixture_id}"
        # )
        # if lineup_errors:
        #     print("lineup errors:", lineup_errors)   

        # 4) fixture_statistic
        # raw_rows, errors = extract_fixture_statistic(fixture_id=fixture_id)
        # print(f"extract fixture statistic {fixture_id}")
            
        # export_json(
        #     data=raw_rows,
        #     prefix=f"fixture_events_l{league_id}_s{season}_f{fixture_id}"
        # )
        # if errors:
        #     print("event errors:", errors)
        
        # 5) fixture_player_statistic
        # fixture_row = fixture_map.get(fixture_id)
        # if not fixture_row:
        #     print(f"no fixture_row found for fixture_id={fixture_id}")
        #     continue

        # team_ids = [
        #     fixture_row.get("home_team_id"),
        #     fixture_row.get("away_team_id"),
        # ]

        # for idx, team_id in enumerate(team_ids):
        #     if not team_id:
        #         continue

        #     side = "home" if idx == 0 else "away"

        #     player_rows, player_errors = extract_fixture_players_statistic(
        #         fixture_id=fixture_id,
        #         team_id=team_id,
        #         side=side
        #     )

        #     export_json(
        #         data=player_rows,
        #         prefix=f"fixture_player_stats_l{league_id}_s{season}_f{fixture_id}_t{team_id}_{side}"
        #     )

        #     if player_errors:
        #         print("player stat errors:", player_errors)

        # 6) extract match predictions 
        # raw_rows, errors = extract_fixture_predictions(fixture_id=fixture_id)
        # print(f"extract match_prediction_{fixture_id}")
            
        # export_json(
        #     data=raw_rows,
        #     prefix=f"match_prediction_{league_id}_s{season}_f{fixture_id}"
        # )
        # if errors:
        #     print("event errors:", errors)
        
        #7 extract fixture odds
        # raw_rows, errors = extract_fixture_odds(fixture_id=fixture_id)
        # print(f"extract_fixture_odds_{fixture_id}")
        # export_json(
        #     data=raw_rows,
        #     prefix=f"fixture_odd_{league_id}_s{season}_f{fixture_id}"
        # )
        # if errors:
        #     print("event errors:", errors)
        