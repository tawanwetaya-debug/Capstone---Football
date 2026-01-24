import kagglehub
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import json

RAW_DIR = Path("data/raw/football_manager")
RAW_DIR.mkdir(parents=True, exist_ok=True)

DATASET_ID = "platinum22/foot-ball-manager-2023-dataset"


def fetch_fm_data():
    """
    Download Football Manager 2023 dataset from Kaggle
    and return local path
    """
    path = kagglehub.dataset_download(DATASET_ID)
    return Path(path)


def extract_and_export():
    dataset_path = fetch_fm_data()
    extracted_at = datetime.now(timezone.utc).isoformat()

    print(f"Dataset downloaded to: {dataset_path}")

    for file in dataset_path.glob("**/*"):
        if file.suffix.lower() not in [".csv", ".parquet"]:
            continue

        print(f"Processing {file.name}")

        df = pd.read_csv(file) if file.suffix == ".csv" else pd.read_parquet(file)

        rows = []
        for _, row in df.iterrows():
            rows.append({
                "source_file": file.name,
                "extracted_at": extracted_at,
                "payload": row.to_dict()
            })

        out_file = RAW_DIR / f"{file.stem}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)

        print(f"Exported {len(rows)} rows → {out_file}")


if __name__ == "__main__":
    extract_and_export()