import json
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import (
    create_engine, text,
    Column, String, Integer, Float, Boolean, DateTime,
    ForeignKey
)
from sqlalchemy.orm import declarative_base, Session

# ── Setup ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

BASE_DIR      = Path(__file__).parent.parent
RAW_DATA_PATH = BASE_DIR / "data" / "raw"
DB_PATH       = BASE_DIR / "data" / "db" / "creator_benchmarker.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

Base = declarative_base()

class Channel(Base):
    """One row per YouTube channel."""
    __tablename__ = "channels"

    channel_id        = Column(String,  primary_key=True)
    title             = Column(String,  nullable=False)
    niche             = Column(String,  nullable=False)
    nlp_niche         = Column(String)
    nlp_confidence    = Column(Float,   default=0.0)
    api_niche         = Column(String)
    niche_match       = Column(Boolean, default=False)
    subscriber_count  = Column(Integer, default=0)
    video_count       = Column(Integer, default=0)
    view_count        = Column(Integer, default=0)
    country           = Column(String,  default="Unknown")
    published_at      = Column(String)
    description       = Column(String)


class Video(Base):
    """One row per YouTube video."""
    __tablename__ = "videos"

    video_id         = Column(String,  primary_key=True)
    channel_id       = Column(String,  ForeignKey("channels.channel_id"))
    niche            = Column(String,  nullable=False)
    title            = Column(String,  nullable=False)
    description      = Column(String)
    published_at     = Column(String)
    duration         = Column(String)
    view_count       = Column(Integer, default=0)
    like_count       = Column(Integer, default=0)
    comment_count    = Column(Integer, default=0)
    engagement_rate  = Column(Float,   default=0.0)
    tags             = Column(String)  # stored as JSON string


class NicheBenchmark(Base):
    """
    Pre-aggregated benchmark stats per niche.
    Recomputed every time load_database.py is run.
    """
    __tablename__ = "niche_benchmarks"

    niche                = Column(String,   primary_key=True)
    avg_subscribers      = Column(Float,    default=0.0)
    avg_views            = Column(Float,    default=0.0)
    avg_engagement_rate  = Column(Float,    default=0.0)
    avg_video_count      = Column(Float,    default=0.0)
    avg_videos_per_month = Column(Float,    default=0.0)
    top_channel          = Column(String)
    total_channels       = Column(Integer,  default=0)
    last_updated         = Column(DateTime)

def load_channels(session: Session, classified: list[dict]) -> None:
    """Insert channel records — skip duplicates."""
    inserted = 0
    skipped  = 0

    for ch in classified:
        existing = session.get(Channel, ch["channel_id"])
        if existing:
            skipped += 1
            continue

        session.add(Channel(
            channel_id       = ch["channel_id"],
            title            = ch["title"],
            niche            = ch["niche"],
            nlp_niche        = ch.get("nlp_niche"),
            nlp_confidence   = ch.get("nlp_confidence", 0.0),
            api_niche        = ch.get("api_niche"),
            niche_match      = ch.get("niche_match", False),
            subscriber_count = ch.get("subscriber_count", 0),
            video_count      = ch.get("video_count", 0),
            view_count       = ch.get("view_count", 0),
            country          = ch.get("country", "Unknown"),
            published_at     = ch.get("published_at", ""),
            description      = ch.get("description", ""),
        ))
        inserted += 1

    session.commit()
    log.info(f"Channels  — inserted: {inserted} | skipped: {skipped}")


def load_videos(session: Session, videos: list[dict]) -> None:
    """Insert video records — skip duplicates."""
    inserted = 0
    skipped  = 0

    for v in videos:
        existing = session.get(Video, v["video_id"])
        if existing:
            skipped += 1
            continue

        session.add(Video(
            video_id        = v["video_id"],
            channel_id      = v["channel_id"],
            niche           = v["niche"],
            title           = v["title"],
            description     = v.get("description", ""),
            published_at    = v.get("published_at", ""),
            duration        = v.get("duration", ""),
            view_count      = v.get("view_count", 0),
            like_count      = v.get("like_count", 0),
            comment_count   = v.get("comment_count", 0),
            engagement_rate = v.get("engagement_rate", 0.0),
            tags            = json.dumps(v.get("tags", [])),
        ))
        inserted += 1

    session.commit()
    log.info(f"Videos    — inserted: {inserted} | skipped: {skipped}")


def compute_niche_benchmarks(session: Session, engine) -> None:
    """
    Compute and insert pre-aggregated benchmark stats per niche.
    Uses pandas + SQL for the heavy lifting.
    This table is dropped and rebuilt fresh every run.
    """
    # Load channels into pandas for aggregation
    channels_df = pd.read_sql("SELECT * FROM channels", engine)
    videos_df   = pd.read_sql("SELECT * FROM videos",   engine)

    # Compute average videos per month per channel
    videos_df["published_at"] = pd.to_datetime(
        videos_df["published_at"], errors="coerce", utc=True
    )
    now = pd.Timestamp.now(tz="UTC")

    # Group videos by channel and compute cadence
    cadence = (
        videos_df.groupby("channel_id")
        .agg(
            earliest_video = ("published_at", "min"),
            video_count    = ("video_id",     "count"),
        )
        .reset_index()
    )
    cadence["months_active"] = (
        (now - cadence["earliest_video"])
        .dt.days / 30
    ).clip(lower=1)
    cadence["videos_per_month"] = (
        cadence["video_count"] / cadence["months_active"]
    ).round(2)

    channels_df = channels_df.merge(
        cadence[["channel_id", "videos_per_month"]],
        on="channel_id", how="left"
    )

    # Aggregate by niche
    benchmarks = (
        channels_df.groupby("niche")
        .agg(
            avg_subscribers     = ("subscriber_count",  "mean"),
            avg_views           = ("view_count",        "mean"),
            avg_video_count     = ("video_count",       "mean"),
            avg_videos_per_month= ("videos_per_month",  "mean"),
            total_channels      = ("channel_id",        "count"),
        )
        .reset_index()
    )

    # Add average engagement rate from videos table
    avg_engagement = (
        videos_df.groupby("niche")["engagement_rate"]
        .mean()
        .reset_index()
        .rename(columns={"engagement_rate": "avg_engagement_rate"})
    )
    benchmarks = benchmarks.merge(avg_engagement, on="niche", how="left")

    # Find top channel per niche by subscriber count
    top_channels = (
        channels_df.loc[
            channels_df.groupby("niche")["subscriber_count"].idxmax()
        ][["niche", "title"]]
        .rename(columns={"title": "top_channel"})
    )
    benchmarks = benchmarks.merge(top_channels, on="niche", how="left")
    benchmarks["last_updated"] = datetime.now(timezone.utc)

    # Round float columns
    float_cols = ["avg_subscribers", "avg_views", "avg_engagement_rate",
                  "avg_video_count", "avg_videos_per_month"]
    benchmarks[float_cols] = benchmarks[float_cols].round(2)

    # Drop and rebuild the benchmarks table
    session.execute(text("DELETE FROM niche_benchmarks"))
    session.commit()

    for _, row in benchmarks.iterrows():
        session.add(NicheBenchmark(
            niche                = row["niche"],
            avg_subscribers      = row["avg_subscribers"],
            avg_views            = row["avg_views"],
            avg_engagement_rate  = row.get("avg_engagement_rate", 0.0),
            avg_video_count      = row["avg_video_count"],
            avg_videos_per_month = row.get("avg_videos_per_month", 0.0),
            top_channel          = row["top_channel"],
            total_channels       = int(row["total_channels"]),
            last_updated         = row["last_updated"],
        ))

    session.commit()
    log.info(f"Benchmarks — computed for {len(benchmarks)} niches")

def verify_database(engine) -> None:
    """Run quick SQL checks to confirm data loaded correctly."""
    checks = {
        "Total channels":   "SELECT COUNT(*) FROM channels",
        "Total videos":     "SELECT COUNT(*) FROM videos",
        "Total niches":     "SELECT COUNT(*) FROM niche_benchmarks",
    }

    log.info("\nDatabase verification:")
    with engine.connect() as conn:
        for label, query in checks.items():
            result = conn.execute(text(query)).scalar()
            log.info(f"  {label:<20} {result}")

        # Show benchmark table
        log.info("\nNiche benchmarks preview:")
        log.info(f"  {'Niche':<12} {'Avg Subs':>12} {'Avg Eng%':>10} {'Channels':>10}")
        log.info(f"  {'-'*48}")
        rows = conn.execute(text(
            "SELECT niche, avg_subscribers, avg_engagement_rate, total_channels "
            "FROM niche_benchmarks ORDER BY avg_subscribers DESC"
        )).fetchall()
        for row in rows:
            log.info(
                f"  {row[0]:<12} {row[1]:>12,.0f} "
                f"{row[2]:>9.2f}% {row[3]:>10}"
            )

# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting database load...")

    # Create engine and tables
    engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
    Base.metadata.create_all(engine)
    log.info(f"Database created at {DB_PATH}")

    # Load raw JSON files
    with open(RAW_DATA_PATH / "channels_classified.json", encoding="utf-8") as f:
        classified = json.load(f)

    with open(RAW_DATA_PATH / "videos_raw.json", encoding="utf-8") as f:
        videos = json.load(f)

    # Load into database
    with Session(engine) as session:
        log.info("Loading channels...")
        load_channels(session, classified)

        log.info("Loading videos...")
        load_videos(session, videos)

        log.info("Computing niche benchmarks...")
        compute_niche_benchmarks(session, engine)

    # Verify everything loaded correctly
    verify_database(engine)
    log.info("\nDone! Run analytics.py next.")

