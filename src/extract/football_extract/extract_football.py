from src.extract.football_api.api_league import fetch_league_data
from src.extract.football_api.api_team import fetch_team_ID_from_League,fetch_team_statistics
from src.extract.football_api.api_player import fetch_team_squad, fetch_player_trophies_bulk,fetch_player_statistics_by_season
from src.extract.football_api.api_transfer import fetch_player_transfer, fetch_team_transfer
from typing import Optional, Tuple, List, Dict, Any
import json
from datetime import datetime, timezone
from pathlib import Path

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
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload" : item 
          }

    )
    return raw_rows

def extract_team_ids(
    league_id: int,
    season: int,
    limit: Optional[int] = None
) -> Tuple[List[int], List[Dict[str, Any]], List[Any]]:

    team_data = fetch_team_ID_from_League(league_id=league_id, season=season)

    data = team_data.get("response", []) or []
    if limit is not None:
        data = data[:limit]

    team_ids: List[int] = []
    team_rows: List[Dict[str, Any]] = []
    errors = team_data.get("errors", []) or team_data.get("error", []) or []

    for item in data:
        team = item.get("team", {}) or {}
        team_id = team.get("id")

        # ✅ check team_id (not team_ids)
        if not team_id:
            continue

        team_ids.append(team_id)
        team_rows.append({
            "league_id": league_id,
            "season": season,
            "team_id": team_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload": item
        })

    return team_ids, team_rows, errors

def extract_team_statistics(
        league_id: int,
        season: int,
        limit: Optional[int] = None
) ->  Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

    stats_rows: List[Dict[str, Any]] = []
    api_errors: List[Dict[str, Any]] = []

    team_ids, team_rows, errors = extract_team_ids(
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
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload": data.get("response", {})
        })

    all_errors = (errors or []) + api_errors
    return stats_rows, all_errors

def extract_team_squad_player_ids(
    team_id: int,
    limit: Optional[int] = None
) -> Tuple[List[int], List[Dict[str, Any]], List[Any]]:

    player_data = fetch_team_squad(team_id=team_id)

    errors = player_data.get("errors", []) or player_data.get("error", []) or []
    response = player_data.get("response", []) or []

    player_ids: List[int] = []
    squad_rows: List[Dict[str, Any]] = []

    source = "https://www.api-football.com/"

    for item in response:
        players = item.get("players", []) or []

        if limit is not None:
            players = players[:limit]

        for p in players:
            player_id = p.get("id")  # ✅ squads schema

            if not player_id:
                continue

            player_ids.append(player_id)
            squad_rows.append({
                "team_id": team_id,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "source": source,
                "payload": players,
            })
    
    return player_ids, squad_rows, errors

# Fetch Player Trophy
def extract_player_trophies_batch(
    team_id: int,
    player_ids: List[int]
) -> List[Dict[str, Any]]:

    data = fetch_player_trophies_bulk(player_ids)
    response = data.get("response", []) or []

    rows: List[Dict[str, Any]] = []

    for item in response:
        rows.append({
            "team_id": team_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload": item
        })

    return rows


def extract_player_trophies_batched(
    player_ids: List[int],
    team_id: int,
    batch_size: int = 20,
    max_players: Optional[int] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

    ids = player_ids[:max_players] if max_players is not None else player_ids

    all_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for batch_idx, batch in enumerate(chunked(ids, batch_size), start=1):
        print(f"[trophy] batch {batch_idx} | players={len(batch)}")

        try:
            batch_rows = extract_player_trophies_batch(team_id,batch)
            all_rows.extend(batch_rows)

        except Exception as e:
            errors.append({
                "batch": batch_idx,
                "player_ids": batch,
                "error": str(e)
            })

    return all_rows, errors


def extract_player_transfer(player_id: int,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:

    player_record = fetch_player_transfer(player_id=player_id)
    data = player_record.get("response", []) or []

    if limit is not None:
        data = data[:limit]

    raw_rows: List[Dict[str, Any]] = []

    for item in data:
        raw_rows.append({
            "player_id": player_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload": item
        })

    return raw_rows

def extract_team_transfer(team_id: int,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:

    team_record = fetch_team_transfer(team_id = team_id)
    data = team_record.get("response", []) or []

    if limit is not None:
        data = data[:limit]

    raw_rows: List[Dict[str, Any]] = []

    for item in data:
        raw_rows.append({
            "team_id": team_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload": item
        })

    return raw_rows

def extract_players_statisitics_byseason(team_id: int,
    season: int,
    league_id: int,
     limit: Optional[int] = None
) -> List[Dict[str, Any]]:

    team_record = fetch_player_statistics_by_season(team_id = team_id,season=season,league_id=league_id)
    if not team_record:
        return []
    
    data = team_record.get("response", []) or []

    if limit is not None:
        data = data[:limit]

    raw_rows: List[Dict[str, Any]] = []

    for item in data:
        raw_rows.append({
            "team_id": team_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "source": "https://www.api-football.com/",
            "payload": item
        })

    return raw_rows

# if __name__ == "__main__":
#     league_id = 39  # Premier League
#     season = 2024
#     limit = 3

    # # # 1. Fetch league metadata
    # league_data = extract_league_data(league_id=league_id, season=season)
    # print("Extract league data")
    # export_json(
    #     data=league_data,
    #     prefix=f"league_l{league_id}_s{season}"
    # )

    # # 2. Extract teams_info and teams_statistics from league
    # stats_rows, errors = extract_team_statistics(
    #     league_id=league_id,
    #     season=season,
    #     limit=limit
    # )

    # print("stats_rows:", len(stats_rows))
    # print("errors:", errors)

    # export_json(data=stats_rows, prefix=f"team_stats_l{league_id}_s{season}")
    # if errors:
    #     export_json(data=errors, prefix=f"errors_team_stats_l{league_id}_s{season}")




# 3. Extract team squad, team transfer, player transfer, player trophy

# team_ids, team_rows, team_errors = extract_team_ids(
#     league_id=league_id,
#     season=season,
#     limit=limit
# )

# print("team_rows:", len(team_rows))
# print("team_errors:", team_errors)

# if team_errors:
#     export_json(data=team_errors, prefix=f"errors_team_ids_l{league_id}_s{season}")

# for team_id in team_ids:
    # --- Team squad ---
    # player_ids, squad_rows, squad_errors = extract_team_squad_player_ids(
    #     team_id=team_id,
    #     limit=limit
    # )

    # print("len player_ids:", len(player_ids))
    # print("len squad_rows:", len(squad_rows))
    # print("first row:", squad_rows[0] if squad_rows else None)

    # export_json(data=player_ids, prefix=f"team_squads_l{league_id}_s{season}_t{team_id}")

    # if squad_errors:
    #     export_json(data=squad_errors, prefix=f"errors_team_squad_l{league_id}_s{season}_t{team_id}")

    # # --- Team transfer (independent from squad errors) ---
    # team_transfer_rows = extract_team_transfer(team_id=team_id)
    # export_json(data=team_transfer_rows, prefix=f"team_transfer_l{league_id}_s{season}_t{team_id}")


    # team_player_trophies, errors = extract_player_trophies_batched(player_ids = player_ids, team_id=team_id)
    # export_json(data=team_player_trophies, prefix=f"player_trophies_team {team_id}")
    # if errors:
    #     export_json(data=errors, prefix=f"player_trophies_team_{team_id}_errors" )

    # # # --- Player-level stuff ---
    # for player_id in player_ids:
    #     # Player transfer
    #     player_transfer_rows = extract_player_transfer(player_id=player_id,limit=limit)
    #     export_json(data=player_transfer_rows, prefix=f"player_transfer_p{player_id}")
    
    # player_statistics = extract_players_statisitics_byseason(season=season,team_id=team_id,league_id=league_id)
    # export_json(data=player_statistics, prefix=f"player_statistics_l{league_id}_s{season}_t{team_id}")

  