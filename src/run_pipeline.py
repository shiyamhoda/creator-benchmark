import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

from config import DB_PATH, RAW_DATA_PATH

def run_pipeline():
    """
    Run the full data pipeline only if the database does not exist.
    This prevents burning API quota on every Render restart.
    """

    if DB_PATH.exists():
        log.info(f"Database already exists at {DB_PATH}")
        log.info("Skipping pipeline — delete the database to force a refresh")
        return

    log.info("Database not found — running full pipeline...")

    # Stage 1 — resolve channel handles to IDs
    log.info("Stage 1: search_channels.py")
    from search_channels import fetch_all_channels as search, save_channel_ids
    from googleapiclient.discovery import build
    from dotenv import load_dotenv
    import os
    load_dotenv()
    from search_channels import build_youtube_client, resolve_all_handles
    youtube = build_youtube_client()
    results = resolve_all_handles(youtube)
    save_channel_ids(results)

    # Stage 2 — fetch channel statistics
    log.info("Stage 2: fetch_channels.py")
    from fetch_channels import load_channel_ids, fetch_all_channels, save_raw_data
    channels_by_niche = load_channel_ids()
    channels = fetch_all_channels(channels_by_niche)
    save_raw_data(channels)

    # Stage 2.5 — fetch video statistics
    log.info("Stage 2.5: fetch_videos.py")
    from fetch_videos import load_channels, fetch_all_videos, save_videos
    channel_list = load_channels()
    videos = fetch_all_videos(channel_list)
    save_videos(videos)

    # Stage 3 — NLP classification
    log.info("Stage 3: classify_niches.py")
    import json
    from classify_niches import classify_all_channels, save_classified
    with open(RAW_DATA_PATH / "channels_raw.json", encoding="utf-8") as f:
        raw_channels = json.load(f)
    classified = classify_all_channels(raw_channels)
    save_classified(classified)

    # Stage 4 — load database
    log.info("Stage 4: load_database.py")
    from sqlalchemy import create_engine
    from load_database import Base, load_channels as db_load_channels
    from load_database import load_videos as db_load_videos
    from load_database import compute_niche_benchmarks
    from sqlalchemy.orm import Session

    engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
    Base.metadata.create_all(engine)

    with open(RAW_DATA_PATH / "channels_classified.json", encoding="utf-8") as f:
        classified_data = json.load(f)
    with open(RAW_DATA_PATH / "videos_raw.json", encoding="utf-8") as f:
        videos_data = json.load(f)

    with Session(engine) as session:
        db_load_channels(session, classified_data)
        db_load_videos(session, videos_data)
        compute_niche_benchmarks(session, engine)

    log.info("Pipeline complete — database ready")


if __name__ == "__main__":
    run_pipeline()