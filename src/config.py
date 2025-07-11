import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Socrata endpoint
IOWA_LIQUOR_API   = os.getenv("IOWA_LIQUOR_API")

# Postgres creds
POSTGRES_HOST     = os.getenv("POSTGRES_HOST")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB       = os.getenv("POSTGRES_DB")
POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# –– ETL tuning ––
CHUNK_ROWS = int(os.getenv("CHUNK_ROWS", 50_000))
TMP_DIR    = Path(os.getenv("TMP_DIR", "/tmp/iowa_liquor_etl"))
TMP_DIR.mkdir(parents=True, exist_ok=True)
