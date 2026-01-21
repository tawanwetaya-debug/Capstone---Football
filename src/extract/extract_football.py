from src.extract.football_api.api_league import fetch_league_data
from src.extract.football_api.api_team import fetch_team_ID_from_League,fetch_team_statistics
from src.extract.football_api.api_player import fetch_team_squad, fetch_player_trophies

from typing import Optional, Tuple, List, Dict, Any
import json
from datetime import datetime
from pathlib import Path

def export_json(data: dict, prefix: str):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    file_path = out_dir / f"{prefix}_{ts}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# Fetch league_data - only interest in league name and season date
def extract_league_data(
        league_id: int,
        season: int,
        limit: Optional[int] = None

) -> List[Dict[str, Any]]:

    league_data = fetch_league_data(league_id=league_id,season=season)
    data = league_data.get("response",[]) or []
    
    if limit is not None:
        data = data[:limit]

    raw_rows = []
    for item in data: 
    
        raw_rows.append(
        {
            "league_id" : league_id,
            "season": season,
            "extracted_at": datetime.utcnow().isoformat(),
            "source": "https://www.api-football.com/",
            "payload" : item 
          }

    )
    return raw_rows

def extract_team_ids(
        league_id: int,
        season: int,
        limit: Optional[int] = None
) -> Tuple[List[int],Dict[str,Any], List[Any]]:
    
    team_data = fetch_team_ID_from_League(league_id=league_id, season=season)
    data = team_data.get("response",[]) or []

    if limit is not None:
        data = data[:limit]

    team_ids: List[int] = []
    team_rows: List[Dict[str, Any]] = []
    errors = team_data.get("errors", []) or team_data.get("error", []) or []

    for item in data:
        team = item.get("team",{}) or {}
        team_id = team.get("id")

        if not team_ids:
            continue
    
        team_ids.append(team_id)
        team_rows.append({
            "league_id": league_id,
            "season": season,
            "team_id": team_id,
            "extracted_at": datetime.utcnow().isoformat(),
            "source": "https://www.api-football.com/",
            "payload": item
            

        })

    return team_ids,team_rows,errors

def extract_team_statistics(
        league_id: int,
        season: int,
        limit: Optional[int] = None
) -> Tuple[List[int],Dict[str,Any], List[Any]]:

    stats_rows: List[Dict[str, Any]] = []
    api_errors: List[Dict[str, Any]] = []

    team_ids, team_row, errors = extract_team_ids(
        league_id=league_id,
        season=season,
        limit=limit
    )

    for team_id in team_ids:
        data = fetch_team_statistics(league_id=league_id,season=season,team_id=team_id)

        if data.get('errors') or data.get('error'):
            api_errors.append({"team_id": team_id, "errors": data["errors"]})
            continue
  
        stats_rows.append({
            "league_id": league_id,
            "season": season,
            "team_id": team_id,
            "extracted_at": datetime.utcnow().isoformat(),
            "source": "https://www.api-football.com/",
            "payload": data.get("response", {})
        })

    all_errors = (errors or []) + api_errors
    return stats_rows, all_errors

# Fetch player_ID from team and season 
def extract_players_ids(
    league_id: int,
    season: int,
    team_id: int,
    limit: Optional[int] = None
) -> Tuple[List[int], List[Dict[str, Any]], List[Any]]:

    player_data = fetch_team_squad(
        league_id=league_id,
        season=season,
        team_id=team_id,
    )

    errors = player_data.get("errors", []) or player_data.get("error", []) or []
    response = player_data.get("response", []) or []

    player_ids: List[int] = []
    squad_rows: List[Dict[str, Any]] = []

    extracted_at = datetime.utcnow().isoformat()
    source = "https://www.api-football.com/"

    # response is usually a list of 1 item containing "players"
    for item in response:
        players = item.get("players", []) or []

        if limit is not None:
            players = players[:limit]

        for p in players:
            player = p.get("player", {}) or {}
            player_id = player.get("id")

            if not player_id:
                continue

            player_ids.append(player_id)

            squad_rows.append({
                "league_id": league_id,
                "season": season,
                "team_id": team_id,
                "player_id": player_id,
                "extracted_at": extracted_at,
                "source": source,
                "payload": p,  # store the per-player payload (cleaner than whole item)
            })

    return player_ids, squad_rows, errors

# Fetch Player Trophy
def extract_player_trophy(player_id: int) -> Tuple [List[Dict[str, Any]]]:
