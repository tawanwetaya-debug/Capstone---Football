git add .env.example
git commit -m "Add env example for configuration"
git push origin main


py -m src.extract.football_extract.extract_football
source .venv/Scripts/activate
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
py -m venv .venv
rm state/cursor.json

dbt ls --resource-type source
dbt run --select stg_league
dbt test --select stg_league
dbt build --select stg_league

set -a
source env.sv
set +a
env | grep SNOWFLAKE