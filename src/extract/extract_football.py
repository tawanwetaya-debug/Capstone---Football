from src.extract.football_api.api_league import fetch_league_data
from src.extract.football_api.api_team import fetch_team_data,fetch_team_ID_from_League,fetch_team_statistics

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

def extract_team_id(
        league_id: int,
        season: int,
        limit: Optional[int] = None
) -> Tuple[List[int],Dict[str,Any], List[Any]]:
    
    team_data = fetch_team_data(league_id=league_id, season=season)
    data = team_data.get("response",[]) or []

    if limit is not None:
        data = data[:limit]

    team_ids: List[int] = []
    team_rows: List[Dict[str, Any]] = []
    errors = team_data.get("errors", []) or team_data.get("error", []) or []

    for item in data:
        team = item.get("team",{}) or {}
        team_id = team.get("id")

        if not team_id:
            continue
    
        team_ids.append(team_id)
        team_rows.append({
            "league_id": league_id,
            "season": season,
            "team_id": team_id,
            "payload": item        
        })

    return team_id,team_rows,errors



