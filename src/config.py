import os
from pathlib import Path

# Works regardless of where the script is called from
# __file__ = src/config.py
# .parent   = src/
# .parent   = project root
PROJECT_ROOT  = Path(__file__).resolve().parent.parent

# Data paths
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
DB_DIR        = PROJECT_ROOT / "data" / "db"
DB_PATH       = DB_DIR / "creator_benchmarker.db"

# Create directories if they don't exist
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
DB_DIR.mkdir(parents=True, exist_ok=True)