import json
from pathlib import Path

# ===== CONFIG =====
STATE_DIR = Path("state")
STATE_DIR.mkdir(exist_ok=True)
CURSOR_FILE = STATE_DIR / "cursor.json"

def load_cursor():
    if not CURSOR_FILE.exists():
        return {
            "league_id": None,
            "season": None,
            "team_id": None,
            "page": 1
        }
    return json.loads(CURSOR_FILE.read_text())

def save_cursor(league_i, season_i, team_i, page_i=1):
    CURSOR_FILE.write_text(json.dumps({
        "league_i": league_i,
        "season_i": season_i,
        "team_i": team_i,
        "page": page_i
    }, indent=2))