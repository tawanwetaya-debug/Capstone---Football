git add .env.example
git commit -m "Add env example for configuration"
git push origin main


py -m src.extract.football_extract.extract_football
source .venv/Scripts/activate
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
py -m venv .venv
rm state/cursor.json
