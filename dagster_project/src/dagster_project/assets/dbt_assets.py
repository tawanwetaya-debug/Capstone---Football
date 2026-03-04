from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

DAGSTER_PROJECT_DIR = Path(__file__).resolve().parents[3]
REPO_ROOT = DAGSTER_PROJECT_DIR.parent
DBT_PROJECT_DIR = REPO_ROOT / "dbt"

DBT_PROFILES_DIR = Path.home() / ".dbt"   # ✅ FIXED
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

dbt = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)

@dbt_assets(manifest=str(DBT_MANIFEST_PATH))
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()