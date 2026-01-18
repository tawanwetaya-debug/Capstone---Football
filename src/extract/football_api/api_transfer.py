import os
from dotenv import load_dotenv
import requests
from typing import Optional, Tuple, List, Dict, Any
from src.extract.base.rate_limiter import rate_limited
from src.extract.base.api_limits import API_SPORTS_MINUTE_LIMITER,API_SPORTS_DAILY_LIMITER

BASE_URL = "https://v3.football.api-sports.io"
# Fetch Team Transfers from Football API
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_team_transfer(team_id: int) -> Dict[str, Any]:
    load_dotenv()
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/transfers"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "team": team_id
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching transfers for team {team_id}: {response.status_code} - {response.text}")
        return None

BASE_URL = "https://v3.football.api-sports.io"
# Fetch Team Transfers from Football API
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_player_transfer(player_id: int) -> Dict[str, Any]:
    load_dotenv()
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/transfers"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "player": player_id
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching transfers for player {player_id}: {response.status_code} - {response.text}")
        return None