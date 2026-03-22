import logging
import subprocess
import sys
from pathlib import Path

# ── Setup ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

from config import DB_PATH, RAW_DATA_PATH

# ── Pipeline definition ───────────────────────────────────────────────────────

SCRIPTS = [
    ("Stage 1 — resolve channel handles", "src/search_channels.py"),
    ("Stage 2 — fetch channel statistics", "src/fetch_channels.py"),
    ("Stage 2.5 — fetch video statistics", "src/fetch_videos.py"),
    ("Stage 3 — NLP classification",       "src/classify_niches.py"),
    ("Stage 4 — load database",            "src/load_database.py"),
]

SPACY_MODEL = "en_core_web_sm"
SPACY_MODEL_PATH = Path(sys.executable).parent.parent / "lib" / "python3.11" / "site-packages" / SPACY_MODEL

# ── Runner ────────────────────────────────────────────────────────────────────

def ensure_spacy_model():
    """Verify spaCy model is available."""
    try:
        import spacy
        spacy.load(SPACY_MODEL)
        log.info(f"spaCy model '{SPACY_MODEL}' is available")
    except OSError:
        log.error(
            f"spaCy model '{SPACY_MODEL}' not found. "
            "It should have been installed via requirements.txt. "
            "Check the build logs."
        )
        sys.exit(1)


def run_pipeline():
    """
    Run the full data pipeline only if the database does not exist.
    Uses subprocess to call each script independently.
    """
    if DB_PATH.exists():
        log.info(f"Database already exists at {DB_PATH}")
        log.info("Skipping pipeline — delete the database file to force a refresh")
        return

    log.info("Database not found — running full pipeline...")

    # Stage 0 — ensure spaCy model is available
    ensure_spacy_model()

    # Stages 1 through 4
    for label, script_path in SCRIPTS:
        log.info(f"Running: {label} ({script_path})")

        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=False,
            text=True,
        )

        if result.returncode != 0:
            log.error(f"Pipeline failed at: {label}")
            log.error(f"Script: {script_path} exited with code {result.returncode}")
            sys.exit(result.returncode)

        log.info(f"Completed: {label}")

    log.info("All pipeline stages complete — database is ready")


if __name__ == "__main__":
    run_pipeline()