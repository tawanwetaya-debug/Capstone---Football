import os
from dotenv import load_dotenv
import requests
from typing import Optional, Tuple, List, Dict, Any
from src.extract.base.rate_limiter import rate_limited
from src.extract.base.api_limits import API_SPORTS_MINUTE_LIMITER,API_SPORTS_DAILY_LIMITER


# Fetch Team Data from Football API

BASE_URL = "https://v3.football.api-sports.io"

@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_team_ID_from_League(league_id: int, season: int) -> Optional[Dict[str, Any]]:
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/teams"
    headers = {
        "x-apisports-key": api_key
    }   
    params = {
        "league": league_id,
        "season": season
    }
    response = requests.get(url, headers=headers, params=params,timeout=30)
    if response.status_code == 200:
        print(f'Beginning fetched data for league {league_id}{season}')
        return response.json()
    else:
        print(f"Error fetching data for league {league_id}: {response.status_code} - {response.text}")
        return None
    

@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_team_statistics(team_id: int, season: int, league_id: int, date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/teams/statistics"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "season": season,
        "team": team_id,
        "league": league_id,

    }

    if date:
        params['date'] = date

    response = requests.get(url, headers=headers, params=params,timeout=30)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching statistics for team {team_id}: {response.status_code} - {response.text}")
        return None

