import os
from dotenv import load_dotenv
import requests
from typing import Optional, Tuple, List, Dict, Any
from src.extract.base.rate_limiter import rate_limited
from src.extract.base.api_limits import API_SPORTS_MINUTE_LIMITER,API_SPORTS_DAILY_LIMITER

BASE_URL = "https://v3.football.api-sports.io"
# Fetch Player from team Squad from Football API
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_team_squad(team_id: int, date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/players/squads"
    headers = {
        "x-apisports-key": api_key
    }   
    params = {
        "team": team_id,
    }

    
    response = requests.get(url, headers=headers, params=params,timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching squad for team {team_id}: {response.status_code} - {response.text}")
        return None
    


#Fetch Player Trophy from Football API
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_player_trophies_bulk(player_ids: List[int]) -> Dict[str, Any]:
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/trophies"
    headers = {
        "x-apisports-key": api_key
    }   
    params = {
        "players": "-".join(map(str, player_ids))
    }
    response = requests.get(url, headers=headers, params=params,timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching trophies for player : {response.status_code} - {response.text}")
        return None

# Fetch Player Statistics by Season 
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_player_statistics_by_season(
    team_id: int,
    season: int,
    league_id: int
) -> Optional[Dict[str, Any]]:

    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")

    url = f"{BASE_URL}/players"
    headers = {"x-apisports-key": api_key}

    all_response = []
    page = 1

    while True:
        params = {
            "team": team_id,
            "league": league_id,
            "season": season,
            "page": page
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            print(
                f"Error fetching players stats "
                f"(team={team_id}, page={page}): "
                f"{response.status_code} - {response.text}"
            )
            break

        payload = response.json()
        data = payload.get("response", [])
        paging = payload.get("paging", {})

        if not data:
            break

        all_response.extend(data)

        if paging.get("current") >= paging.get("total"):
            break

        page += 1

    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "results": len(all_response),
        "response": all_response
    }
    