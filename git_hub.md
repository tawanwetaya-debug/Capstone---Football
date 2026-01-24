git add .env.example
git commit -m "Add env example for configuration"
git push origin main


python -m src.extract.football_extract.extract_football
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt