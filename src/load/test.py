from pathlib import Path
import json

# always show absolute path to avoid editing the wrong file
CURSOR_PATH = Path("state/cursor.json")

def load_cursor():
    print("LOAD CURSOR FROM:", CURSOR_PATH.resolve())

    if not CURSOR_PATH.exists():
        print("CURSOR FILE NOT FOUND -> start fresh")
        return {}

    data = json.loads(CURSOR_PATH.read_text(encoding="utf-8"))
    print("CURSOR LOADED:", data)
    return data

def save_cursor(league_i, season_i, team_i, page_i=None, stage=None):
    payload = {"league_i": league_i, "season_i": season_i, "team_i": team_i}
    if page_i is not None:
        payload["page_i"] = page_i
    if stage is not None:
        payload["stage"] = stage

    print("SAVE CURSOR TO:", CURSOR_PATH.resolve())
    print("CURSOR SAVING:", payload)

    CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    CURSOR_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

def main():
    # ✅ quick test: load cursor and show start values
    cursor = load_cursor()

    start_lg_i = cursor.get("league_i", 0)
    start_ss_i = cursor.get("season_i", 0)
    start_team_i = cursor.get("team_i", 0)
    start_page_i = cursor.get("page_i", 1)
    stage = cursor.get("stage", "league_info")

    print("START:", start_lg_i, start_ss_i, start_team_i, start_page_i, stage)

    # ✅ optional: force reset (run once, then comment out)
    # save_cursor(0, 0, 0, 1, "league_info")

if __name__ == "__main__":
    main()