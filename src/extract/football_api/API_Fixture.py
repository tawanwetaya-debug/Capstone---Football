import os
from dotenv import load_dotenv
import requests
from typing import Optional, Tuple, List, Dict, Any
from src.extract.base.rate_limiter import rate_limited
from src.extract.base.api_limits import API_SPORTS_MINUTE_LIMITER,API_SPORTS_DAILY_LIMITER

BASE_URL = "https://v3.football.api-sports.io"

# Fetch Fixture Data from Football API
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_fixture_data(season: int, league_id: int, date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/fixtures"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "season": season,
        "league": league_id,
        "date":date,
    }

    if date:
        params['date'] = date

    response = requests.get(url, headers=headers, params=params, timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching historical data for season {season}, league {league_id}: {response.status_code} - {response.text}")
        return None


# Fetch Fixtures by Event by Fixture ID from Football API
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_fixture_events(fixture_id: int) -> Optional[Dict[str, Any]]:
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/fixtures/events"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "fixture": fixture_id
    }
    response = requests.get(url, headers=headers, params=params,timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching events for fixture {fixture_id}: {response.status_code} - {response.text}")
        return None

# Fetch Fixtures by Lineups by Fixture ID from Football API
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_fixture_lineups(fixture_id: int) -> Optional[Dict[str, Any]]:
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/fixtures/lineups"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "fixture": fixture_id
    }
    response = requests.get(url, headers=headers, params=params,timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching lineups for fixture {fixture_id}: {response.status_code} - {response.text}")
        return None

# Fetch Team Statisics by Fixture ID
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_fixture_statistic(fixture_id: int) -> Optional[Dict[str, Any]]:
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/fixtures/statistics"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "fixture": fixture_id,
    }    
    response = requests.get(url, headers=headers, params=params, timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching players statistic for fixture {fixture_id}: {response.status_code} - {response.text}")
        return None

# Fetch Player Statistic 
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_players_statistic(fixture_id: int, team_id = int) -> Optional[Dict[str, Any]]:
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/fixtures/players"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "fixture": fixture_id,
        "team": team_id
    }
    response = requests.get(url, headers=headers, params=params, timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching players statistic for fixture {fixture_id}: {response.status_code} - {response.text}")
        return None

# Fetch Match Prediction by Fixture ID 
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_match_prediction(fixture_id: int):
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/predictions"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "fixture": fixture_id
    }
    response = requests.get(url, headers=headers, params=params, timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching match prediction {fixture_id}: {response.status_code} - {response.text}")
        return None    

#Fetch Live Odd by Fixture ID
@rate_limited(API_SPORTS_DAILY_LIMITER)
@rate_limited(API_SPORTS_MINUTE_LIMITER)
def fetch_match_odd(fixture_id: int):
    load_dotenv("env.sv")
    api_key = os.getenv("FOOTBALL_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set FOOTBALL_API_KEY in your environment variables.")
    url = f"{BASE_URL}/odds"
    headers = {
        "x-apisports-key": api_key
    }
    params = {
        "fixture": fixture_id
    }
    response = requests.get(url, headers=headers, params=params, timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching odd betting {fixture_id}: {response.status_code} - {response.text}")
        return None    
    

    
    
